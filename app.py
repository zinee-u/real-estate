"""
분당구 야탑동 아파트 시세 추적 대시보드
Zigbang 데이터를 활용하여 시세·매물을 정기 수집합니다.
"""

import csv
import json
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, unquote
from dotenv import load_dotenv

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, make_response, render_template, request, send_from_directory
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Set, Tuple

# ===== 설정 =====
load_dotenv()

DB_NAME = os.getenv("ESTATE_DB", "yatap_apt.db")
TARGET_QUERY = os.getenv("ESTATE_TARGET_QUERY", "야탑동")
MAX_COMPLEXES = int(os.getenv("ESTATE_MAX_COMPLEXES", "12"))
CRAWL_DELAY = float(os.getenv("ESTATE_CRAWL_DELAY", "0.8"))
USER_AGENT = os.getenv(
    "ESTATE_USER_AGENT",
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
)
LOAN_LTV = float(os.getenv("ESTATE_LOAN_LTV", "0.6"))  # 담보인정비율 기본 60%
LOAN_INTEREST = float(os.getenv("ESTATE_LOAN_INTEREST", "0.042"))  # 연이율
LOAN_YEARS = int(os.getenv("ESTATE_LOAN_YEARS", "30"))
# 지역 실거래 조회용 국토부 API 키 (환경 변수 우선, 없으면 기본값 사용)
RAW_DATA_API_KEY = os.getenv("DATA_APP_KEY")
DATA_API_KEY = unquote(RAW_DATA_API_KEY.strip()) if RAW_DATA_API_KEY else ""
# The MOLIT 실거래 API is also exposed via data.go.kr. Prefer that domain so we can
# piggy-back on their wider CDN footprint, but keep the legacy openapi.molit.go.kr
# hosts as a fallback for older environments.
MOLIT_API_BASES = [
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade",
    "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade",
    "https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc",
    "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc",
]
MOLIT_APT_TRADE_PATH = "getRTMSDataSvcAptTrade"
# MOLIT 성공 코드는 게이트웨이에 따라 "00" 또는 "000"을 사용한다.
MOLIT_SUCCESS_CODES = {"00", "000", "0"}
LAWD_CODE_MAP = {
    "분당구": "41135",
    "용인시 수지구": "41465",
    "수원시 영통구": "41117",
    "성남시 수정구": "41131",
    "수지구": "41465",
    "영통구": "41117",
    "수정구": "41131",
}
EXPORT_DIR = Path(__file__).parent / "exports"

def normalize_apt_name(name: str) -> str:
    """Normalize apartment name for matching."""
    name = name.lower()
    # Remove common suffixes and special characters
    name = re.sub(r"\(.*?\)|\[.*?\]", "", name) # remove content in brackets
    name = re.sub(r"\d+(차|단지)", "", name) # remove N-cha, N-danji
    name = name.replace("아파트", "").replace("e편한세상", "이편한세상").strip()
    name = re.sub(r"[^\w가-힣]", "", name) # remove non-alphanumeric
    return name


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("estate")


# ===== 공용 유틸 =====
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" in str(exc):
            return
        raise


def init_db() -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT UNIQUE,
            apt_complex TEXT,
            apt_name TEXT,
            price INTEGER,
            price_type TEXT,
            area REAL,
            area_type TEXT,
            floor TEXT,
            direction TEXT,
            listing_date TEXT,
            trade_date TEXT,
            source TEXT,
            url TEXT,
            area_ho_id INTEGER,
            listing_tran_type TEXT,
            is_new INTEGER DEFAULT 1,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            monthly_rent INTEGER DEFAULT NULL,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS price_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apt_complex TEXT,
            area_type TEXT,
            max_price INTEGER,
            min_area REAL,
            max_area REAL,
            notify_new BOOLEAN DEFAULT 1,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT,
            new_count INTEGER,
            total_count INTEGER,
            error_msg TEXT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS room_type_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apt_complex TEXT,
            complex_id INTEGER,
            room_type_id INTEGER,
            room_type_name TEXT,
            area_m2 REAL,
            area_pyeong REAL,
            sales_min INTEGER,
            sales_max INTEGER,
            rent_min INTEGER,
            rent_max INTEGER,
            captured_date TEXT,
            captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_url TEXT,
            UNIQUE(apt_complex, room_type_id, captured_date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apt_complex TEXT,
            series_label TEXT,
            metric_type TEXT,
            price INTEGER,
            date TEXT,
            captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(apt_complex, series_label, metric_type, date)
        )
        """
    )

    ensure_column(cur, "listings", "monthly_rent", "INTEGER")
    ensure_column(cur, "listings", "last_seen", "TIMESTAMP")
    ensure_column(cur, "listings", "area_ho_id", "INTEGER")
    ensure_column(cur, "listings", "listing_tran_type", "TEXT")
    cur.execute("UPDATE listings SET last_seen = COALESCE(last_seen, collected_at)")
    ensure_column(cur, "room_type_prices", "complex_id", "INTEGER")
    ensure_column(cur, "room_type_prices", "source_url", "TEXT")

    conn.commit()
    conn.close()


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def price_to_uk(value: Optional[int]) -> str:
    if value is None:
        return "-"
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        return "-"
    if ivalue >= 10000:
        return f"{ivalue / 10000:.1f}억"
    return f"{ivalue:,}만"


def format_change(value: Optional[int], percent: Optional[float]) -> str:
    if value is None or percent is None:
        return "변동 없음"
    arrow = "▲" if value > 0 else ("▼" if value < 0 else "―")
    abs_value = abs(value)
    abs_percent = abs(percent)
    if abs_value >= 100:
        amount = f"{abs_value / 10000:.1f}억"
    else:
        amount = f"{abs_value:,}만"
    return f"{arrow} {amount} ({abs_percent:.1f}%)"


def loan_summary_from_price(price_man: Optional[int]) -> Dict[str, Optional[Any]]:
    """대출 가능 금액 및 월 상환액 계산 (원 단위)."""
    if price_man is None:
        return {"loan_amount": "-", "monthly_payment": "-", "loan_amount_man": None}
    principal = price_man * 10000 * LOAN_LTV  # price_man in 만원
    if principal <= 0:
        return {"loan_amount": "-", "monthly_payment": "-", "loan_amount_man": None}
    monthly_rate = LOAN_INTEREST / 12
    total_months = LOAN_YEARS * 12
    if monthly_rate == 0:
        payment = principal / total_months
    else:
        payment = principal * (monthly_rate * (1 + monthly_rate) ** total_months) / (
            (1 + monthly_rate) ** total_months - 1
        )
    loan_amount_display = f"{principal/100000000:.2f}억"
    monthly_payment_display = f"{payment/10000:,.0f}만/월"
    loan_amount_man = int(round(principal / 10000))
    return {
        "loan_amount": loan_amount_display,
        "monthly_payment": monthly_payment_display,
        "loan_amount_man": loan_amount_man,
    }


ROOM_LABEL_NORMALIZE_RE = re.compile(r"[^0-9a-zA-Z가-힣]+")


def normalize_room_label(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    normalized = ROOM_LABEL_NORMALIZE_RE.sub("", str(value))
    return normalized.lower() if normalized else None


def compute_representative_price(min_value: Optional[int], max_value: Optional[int]) -> Optional[int]:
    values = [val for val in (min_value, max_value) if val is not None]
    if not values:
        return None
    if len(values) == 2:
        return int(round((values[0] + values[1]) / 2))
    return int(values[0])


def build_room_type_match_keys(
    name: Optional[str], area_m2: Optional[float], area_pyeong: Optional[float]
) -> Set[str]:
    keys: Set[str] = set()
    candidates: List[Any] = []
    if name:
        candidates.append(name)
    if area_m2:
        candidates.extend(
            [
                f"{area_m2:.1f}",
                f"{area_m2:.0f}",
                f"{area_m2:.1f}㎡",
                f"{area_m2:.0f}㎡",
            ]
        )
    if area_pyeong:
        candidates.extend(
            [
                f"{area_pyeong:.1f}",
                f"{area_pyeong:.0f}",
                f"{area_pyeong:.1f}평",
                f"{area_pyeong:.0f}평",
            ]
        )
    for candidate in candidates:
        normalized = normalize_room_label(candidate)
        if normalized:
            keys.add(normalized)
    return keys


def find_room_type_match(
    candidates: List[Dict[str, Any]], area_type: Optional[str], area_m2_value: Optional[float]
) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    normalized_target = normalize_room_label(area_type)
    if normalized_target:
        for candidate in candidates:
            match_keys: Set[str] = candidate.get("match_keys", set())
            if normalized_target in match_keys:
                return candidate
    if area_m2_value is not None:
        best_candidate: Optional[Dict[str, Any]] = None
        best_diff: Optional[float] = None
        for candidate in candidates:
            candidate_area = candidate.get("area_m2")
            if candidate_area is None:
                continue
            diff = abs(candidate_area - area_m2_value)
            if best_diff is None or diff < best_diff:
                best_candidate = candidate
                best_diff = diff
        if best_candidate is not None and best_diff is not None and best_diff <= 1.0:
            return best_candidate
    return None


def request_molit_api(
    params: Dict[str, Any], api_path: str = MOLIT_APT_TRADE_PATH
) -> requests.Response:
    """Call the MOLIT API, trying HTTP fallback endpoints if needed."""

    last_error: Optional[Exception] = None
    last_endpoint: Optional[str] = None
    attempted: List[str] = []
    for base in MOLIT_API_BASES:
        endpoint = f"{base}/{api_path}"
        attempted.append(endpoint)
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            logger.debug("MOLIT request succeeded via %s", endpoint)
            return response
        except requests.exceptions.SSLError as exc:
            # SSL errors are terminal for HTTPS, so try next endpoint (likely HTTP)
            last_error = exc
            last_endpoint = endpoint
            logger.warning("SSL error on %s: %s", endpoint, exc)
        except requests.RequestException as exc:  # network / HTTP error
            last_error = exc
            last_endpoint = endpoint
            logger.warning("MOLIT API request failed via %s: %s", endpoint, exc)
            time.sleep(0.2)
    attempted_text = ", ".join(attempted)
    raise RuntimeError(
        f"MOLIT API 요청 실패({last_endpoint or 'unknown endpoint'}): {last_error}. 시도한 게이트웨이: {attempted_text}"
    )


def find_first_text(element: ET.Element, *tag_names: str) -> Optional[str]:
    """Return the first non-empty text across the provided XML tag names."""

    for tag in tag_names:
        if not tag:
            continue
        value = element.findtext(tag)
        if value is not None and value.strip() != "":
            return value
    return None


def is_molit_success(result_code: Optional[str]) -> bool:
    """Return True when the MOLIT API returned a documented success code."""

    if result_code is None:
        return True
    return result_code in MOLIT_SUCCESS_CODES


def iter_recent_months(years: int = 7) -> List[str]:
    """Yield year-month strings (YYYYMM) for the recent `years` years including this month."""

    results: List[str] = []
    today = datetime.now()
    year = today.year
    month = today.month

    for _ in range(years * 12):
        results.append(f"{year}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return results


def fetch_molit_data(region_name: str, period_months: int = 84) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch real estate transaction data from MOLIT API."""
    lawd_code = None
    for key, code in LAWD_CODE_MAP.items():
        if region_name in key:
            lawd_code = code
            break

    if not lawd_code:
        logger.warning("No LAWD_CODE found for region: %s", region_name)
        return {}

    today = datetime.now()

    all_data: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for i in range(period_months):
        target_date = today - timedelta(days=i * 30)
        deal_ymd = target_date.strftime("%Y%m")
        
        params = {
            "serviceKey": DATA_API_KEY,
            "LAWD_CD": lawd_code,
            "DEAL_YMD": deal_ymd,
            "numOfRows": "1000",
        }

        try:
            response = request_molit_api(params, MOLIT_APT_TRADE_PATH)
            root = ET.fromstring(response.content)
            result_code = root.findtext(".//resultCode")
            if not is_molit_success(result_code):
                result_msg = root.findtext(".//resultMsg") or "알 수 없는 오류"
                logger.error(
                    "MOLIT API error for %s (%s): %s", deal_ymd, result_code, result_msg
                )
                continue

            for item in root.findall(".//item"):
                apt_name_raw = find_first_text(item, "아파트", "aptNm")
                if not apt_name_raw:
                    continue

                apt_name = apt_name_raw.strip()
                trade_price_text = find_first_text(item, "거래금액", "dealAmount") or "0"
                trade_price_str = trade_price_text.strip().replace(",", "")
                area_text = find_first_text(item, "전용면적", "excluUseAr")

                year_text = find_first_text(item, "년", "dealYear") or ""
                month_text = find_first_text(item, "월", "dealMonth") or ""
                day_text = find_first_text(item, "일", "dealDay") or ""

                all_data[apt_name].append(
                    {
                        "date": f"{year_text}-{month_text.zfill(2)}-{day_text.zfill(2)}",
                        "price": int(trade_price_str),
                        "area": safe_float(area_text),
                    }
                )
        except (requests.RequestException, ET.ParseError) as e:
            logger.error("Failed to fetch or parse MOLIT data for %s: %s", deal_ymd, e)
            continue
        time.sleep(0.2) # API rate limit

    # Sort data by date
    for apt in all_data:
        all_data[apt].sort(key=lambda x: x["date"])
        
    return all_data


def format_trade_price(value: Optional[int]) -> str:
    if value is None:
        return "-"
    return f"{value:,} 만원"


def fetch_molit_transactions_by_code(lawd_code: str, deal_ym: str) -> List[Dict[str, Any]]:
    """Fetch MOLIT transactions for a specific region and month."""
    if not DATA_API_KEY:
        raise RuntimeError("DATA_API_KEY 환경 변수가 설정되지 않았습니다.")

    num_rows = 500
    page = 1
    transactions: List[Dict[str, Any]] = []

    while True:
        params = {
            "serviceKey": DATA_API_KEY,
            "LAWD_CD": lawd_code,
            "DEAL_YMD": deal_ym,
            "numOfRows": str(num_rows),
            "pageNo": str(page),
        }

        response = request_molit_api(params, MOLIT_APT_TRADE_PATH)
        root = ET.fromstring(response.content)

        result_code = root.findtext(".//resultCode")
        if not is_molit_success(result_code):
            result_msg = root.findtext(".//resultMsg") or "알 수 없는 오류"
            raise RuntimeError(f"MOLIT API 오류({result_code}): {result_msg}")

        items = root.findall(".//item")
        if not items:
            break

        for item in items:
            apt_name = (find_first_text(item, "아파트", "aptNm") or "").strip()
            if not apt_name:
                continue
            trade_price_str = (
                (find_first_text(item, "거래금액", "dealAmount") or "0").strip().replace(",", "")
            )
            trade_price = safe_int(trade_price_str)
            area = safe_float(find_first_text(item, "전용면적", "excluUseAr"))
            legal_dong = (find_first_text(item, "법정동", "umdNm") or "").strip()
            floor = find_first_text(item, "층", "floor")
            build_year = find_first_text(item, "건축년도", "buildYear")
            year = find_first_text(item, "년", "dealYear") or ""
            month = find_first_text(item, "월", "dealMonth") or ""
            day = find_first_text(item, "일", "dealDay") or ""
            deal_type = find_first_text(item, "거래유형", "tradeType") or "일반"
            date = (
                f"{year}-{month.zfill(2)}-{day.zfill(2)}" if year and month and day else None
            )

            transactions.append(
                {
                    "apartment": apt_name,
                    "price": trade_price,
                    "price_display": format_trade_price(trade_price),
                    "area": area,
                    "legal_dong": legal_dong,
                    "floor": floor,
                    "build_year": build_year,
                    "deal_type": deal_type,
                    "date": date,
                }
            )

        if len(items) < num_rows:
            break
        page += 1

    transactions.sort(key=lambda item: item.get("date") or "", reverse=True)
    return transactions


def export_molit_history_csv(
    lawd_code: str, pyeong_min: float, pyeong_max: float, years: int = 7
) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Download recent years of MOLIT trades, filter by pyeong range, save CSV, and prep chart data."""

    if not DATA_API_KEY:
        raise RuntimeError("DATA_API_KEY 환경 변수가 설정되지 않았습니다.")

    os.makedirs(EXPORT_DIR, exist_ok=True)

    csv_rows: List[Dict[str, Any]] = []
    monthly_prices: Dict[str, List[int]] = defaultdict(list)

    for deal_ym in iter_recent_months(years):
        try:
            month_transactions = fetch_molit_transactions_by_code(lawd_code, deal_ym)
        except RuntimeError as exc:
            logger.warning("Skipping %s due to MOLIT API error: %s", deal_ym, exc)
            continue

        for tx in month_transactions:
            area_m2 = tx.get("area")
            price = tx.get("price")
            if area_m2 is None or price is None:
                continue

            pyeong_value = area_m2 * 0.3025
            if pyeong_value < pyeong_min or pyeong_value > pyeong_max:
                continue

            date_value = tx.get("date") or f"{deal_ym[:4]}-{deal_ym[4:]}-01"
            csv_rows.append(
                {
                    "거래일": date_value,
                    "아파트": tx.get("apartment", ""),
                    "법정동": tx.get("legal_dong", ""),
                    "전용면적(㎡)": f"{area_m2:.2f}",
                    "전용면적(평)": f"{pyeong_value:.2f}",
                    "층": tx.get("floor") or "",
                    "건축년도": tx.get("build_year") or "",
                    "거래유형": tx.get("deal_type") or "",
                    "거래금액(만원)": price,
                }
            )
            month_key = date_value[:7]
            monthly_prices[month_key].append(price)

    if not csv_rows:
        return None, []

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"molit_{lawd_code}_{int(pyeong_min)}-{int(pyeong_max)}p_{timestamp}.csv"
    filepath = EXPORT_DIR / filename

    with filepath.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "거래일",
                "아파트",
                "법정동",
                "전용면적(㎡)",
                "전용면적(평)",
                "층",
                "건축년도",
                "거래유형",
                "거래금액(만원)",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    chart_points = [
        {
            "month": month,
            "avg_price": int(sum(values) / len(values)),
            "count": len(values),
        }
        for month, values in sorted(monthly_prices.items())
    ]

    return filename, chart_points



# ===== Zigbang 크롤러 =====
class ZigbangCrawler:
    SEARCH_URL = "https://apis.zigbang.com/v2/search"
    BASE_URL = "https://m.zigbang.com"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Connection": "keep-alive",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        self.build_id: Optional[str] = None

    def get_build_id(self) -> str:
        if self.build_id:
            return self.build_id
        resp = self.session.get(self.BASE_URL, timeout=10)
        resp.raise_for_status()
        match = re.search(r'"buildId":"([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("Unable to locate Zigbang buildId")
        self.build_id = match.group(1)
        return self.build_id

    def search_apartments(self, query: str = TARGET_QUERY, limit: int = MAX_COMPLEXES) -> List[Dict[str, Any]]:
        resp = self.session.get(self.SEARCH_URL, params={"q": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        apartments: List[Dict[str, Any]] = []
        for item in data.get("items", []):
            if item.get("type") != "apartment":
                continue
            desc = item.get("description", "")
            if query not in desc and query not in item.get("name", ""):
                continue
            apartments.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "description": desc,
                    "lat": item.get("lat"),
                    "lng": item.get("lng"),
                }
            )
        apartments.sort(key=lambda entry: entry["name"])
        return apartments[:limit]

    def fetch_complex_snapshot(self, danji_id: int) -> Dict[str, Any]:
        build_id = self.get_build_id()
        url = f"{self.BASE_URL}/_next/data/{build_id}/home/apt/danjis/{danji_id}.json"
        headers = {"Referer": f"{self.BASE_URL}/apt/danjis/{danji_id}"}
        resp = self.session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json().get("pageProps", {}).get("SSRData", {})


# ===== 데이터 저장 =====
def store_room_type_prices(conn: sqlite3.Connection, apt_name: str, snapshot: Dict[str, Any]) -> int:
    room_types = snapshot.get("danjisRoomTypes", {}).get("room_types", [])
    if not room_types:
        return 0
    cur = conn.cursor()
    captured_date = datetime.now().strftime("%Y-%m-%d")
    inserted = 0
    complex_id = snapshot.get("danjis", {}).get("id")
    source_url = f"https://www.zigbang.com/home/search?type=apt&q={quote_plus(apt_name)}"
    for room in room_types:
        room_type_id = room.get("id")
        if room_type_id is None:
            continue
        room_type_name = room.get("room_type_title", {}).get("p") or room.get("room_type_title", {}).get("m2")
        area_m2 = (
            room.get("전용면적", {}).get("m2")
            or room.get("sizeM2")
            or room.get("공급면적", {}).get("m2")
        )
        area_pyeong = (
            room.get("전용면적", {}).get("p")
            or room.get("room_type_title", {}).get("p")
            or room.get("공급면적", {}).get("p")
        )
        sales_price = room.get("sales_price") or {}
        rent_price = room.get("rent_price") or {}
        cur.execute(
            """
            INSERT OR REPLACE INTO room_type_prices
            (apt_complex, complex_id, room_type_id, room_type_name, area_m2, area_pyeong,
             sales_min, sales_max, rent_min, rent_max, captured_date, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                apt_name,
                complex_id,
                room_type_id,
                room_type_name,
                safe_float(area_m2),
                safe_float(area_pyeong),
                safe_int(sales_price.get("min")),
                safe_int(sales_price.get("max")),
                safe_int(rent_price.get("min")),
                safe_int(rent_price.get("max")),
                captured_date,
                source_url,
            ),
        )
        inserted += 1
    return inserted


def store_price_history(conn: sqlite3.Connection, apt_name: str, snapshot: Dict[str, Any]) -> int:
    chart = snapshot.get("danjis", {}).get("danjiPriceChart", {})
    series_list = chart.get("list", [])
    if not series_list:
        return 0
    cur = conn.cursor()
    inserted = 0
    timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")
    for series in series_list:
        label = series.get("label")
        metric_type = series.get("type") or "sale"
        if not label:
            continue
        for point in series.get("data", []):
            price = safe_int(point.get("price"))
            date_value = point.get("date")
            if date_value is None:
                continue
            cur.execute(
                """
                INSERT OR REPLACE INTO price_history
                (apt_complex, series_label, metric_type, price, date, captured_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (apt_name, label, metric_type, price, date_value, timestamp),
            )
            inserted += 1
    return inserted


def store_molit_data(conn: sqlite3.Connection, molit_data: Dict[str, List[Dict[str, Any]]]) -> int:
    """Store MOLIT transaction data into the price_history table."""
    cur = conn.cursor()
    inserted = 0
    timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")
    
    # Get existing apartment names from the database to match against
    cur.execute("SELECT DISTINCT apt_complex FROM room_type_prices")
    db_apt_names = [row[0] for row in cur.fetchall()]
    
    # Create a map from normalized name to original DB name
    normalized_to_db_name = {normalize_apt_name(name): name for name in db_apt_names}
    
    for apt_name_raw, records in molit_data.items():
        normalized_molit_name = normalize_apt_name(apt_name_raw)
        
        # Find the matching database apartment name
        db_apt_name = normalized_to_db_name.get(normalized_molit_name)
        if not db_apt_name:
            # A simple heuristic for partial matches if no direct one is found
            for norm_db, orig_db in normalized_to_db_name.items():
                if normalized_molit_name in norm_db or norm_db in normalized_molit_name:
                    db_apt_name = orig_db
                    break

        if not db_apt_name:
            logger.debug("No matching complex in DB for MOLIT apt: %s", apt_name_raw)
            continue

        for record in records:
            price = record.get("price")
            date = record.get("date")
            if not price or not date:
                continue
            
            # We can add more logic here to match by area if needed
            # For now, we use a generic label for the complex
            series_label = "실거래"
            
            cur.execute(
                """
                INSERT OR REPLACE INTO price_history
                (apt_complex, series_label, metric_type, price, date, captured_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (db_apt_name, series_label, "trade", price, date, timestamp),
            )
            inserted += 1
            
    logger.info("Stored %d MOLIT transaction records.", inserted)
    return inserted


def store_listings(conn: sqlite3.Connection, apt_name: str, snapshot: Dict[str, Any]) -> int:
    items = snapshot.get("itemCatalog", {}).get("list", [])
    if not items:
        return 0
    cur = conn.cursor()
    now_ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    new_or_changed = 0
    for item in items:
        identifier_list = item.get("itemIdList") or []
        if not identifier_list:
            continue
        primary = identifier_list[0]
        listing_id = f"{primary.get('itemSource', 'zigbang')}_{primary.get('itemId')}"
        tran_type = item.get("tranType")
        trade_price = item.get("tradePriceMin") or item.get("salePriceMin") or item.get("priceMin")
        deposit = safe_int(item.get("depositMin"))
        rent = safe_int(item.get("rentMin"))
        area_ho_id = item.get("areaHoId")
        price_type = "매매" if tran_type == "trade" else "전세"
        if tran_type == "trade" and trade_price is not None:
            deposit = safe_int(trade_price)
            rent = None
        elif tran_type == "rental":
            price_type = "전세" if not rent else "월세"
        area = safe_float(item.get("sizeM2"))
        area_type = item.get("roomTypeTitle", {}).get("p") or item.get("roomTypeTitle", {}).get("m2")
        dong = item.get("dong") or ""
        floor = item.get("floor")
        if floor is not None and floor != "":
            floor_display = f"{dong} {floor}층".strip()
        else:
            floor_display = dong or None
        url = None
        web_url = None
        if primary.get("itemId"):
            item_id = primary["itemId"]
            url = f"https://m.zigbang.com/home/apt/item/{item_id}"
            web_url = f"https://www.zigbang.com/home/apt/item/{item_id}"
        cur.execute(
            "SELECT price, monthly_rent, collected_at FROM listings WHERE listing_id = ?",
            (listing_id,),
        )
        existing = cur.fetchone()
        if existing is None:
            cur.execute(
                """
                INSERT INTO listings
                (listing_id, apt_complex, apt_name, price, price_type, area, area_type,
                 floor, direction, listing_date, trade_date, source, url, area_ho_id,
                 listing_tran_type, is_new, collected_at, monthly_rent, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    listing_id,
                    apt_name,
                    apt_name,
                    deposit,
                    price_type,
                    area,
                    area_type,
                    floor_display,
                    item.get("itemType"),
                    datetime.now().strftime("%Y-%m-%d"),
                    None,
                    "zigbang",
                    url,
                    area_ho_id,
                    tran_type,
                    1,
                    now_ts,
                    rent,
                    now_ts,
                ),
            )
            new_or_changed += 1
        else:
            prev_price, prev_rent, prev_collected = existing
            changed = (prev_price != deposit) or ((prev_rent or 0) != (rent or 0))
            cur.execute(
                """
                UPDATE listings
                SET price = ?, price_type = ?, area = ?, area_type = ?, floor = ?, direction = ?,
                    source = ?, url = ?, area_ho_id = ?, listing_tran_type = ?, monthly_rent = ?,
                    last_seen = ?, collected_at = ?, is_new = ?
                WHERE listing_id = ?
                """,
                (
                    deposit,
                    price_type,
                    area,
                    area_type,
                    floor_display,
                    item.get("itemType"),
                    "zigbang",
                    url,
                    area_ho_id,
                    tran_type,
                    rent,
                    now_ts,
                    now_ts if changed else prev_collected,
                    1 if changed else 0,
                    listing_id,
                ),
            )
            if changed:
                new_or_changed += 1
    return new_or_changed


def record_crawl_result(
    conn: sqlite3.Connection,
    status: str,
    new_listings: int,
    room_type_count: int,
    price_points: int,
    complex_count: int,
    error_msg: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_log (status, new_count, total_count, error_msg)
        VALUES (?, ?, ?, ?)
        """,
        (status, new_listings, room_type_count, error_msg),
    )
    logger.info(
        "crawl summary status=%s complexes=%d room_types=%d price_points=%d new_or_changed=%d",
        status,
        complex_count,
        room_type_count,
        price_points,
        new_listings,
    )


def reset_new_listing_flags(conn: sqlite3.Connection, hours: int = 1) -> None:
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat(sep=" ", timespec="seconds")
    conn.execute(
        "UPDATE listings SET is_new = 0 WHERE is_new = 1 AND datetime(collected_at) < datetime(?)",
        (cutoff,),
    )


# ===== 크롤링 & 알림 =====
def crawl_job(query: str = TARGET_QUERY) -> int:
    logger.info("Starting crawl job for query '%s'", query)
    crawler = ZigbangCrawler()
    conn = get_db_connection()
    new_listings = 0
    room_type_count = 0
    price_points = 0
    complex_count = 0
    molit_points = 0
    try:
        # 1. Fetch Zigbang data
        complexes = crawler.search_apartments(query)
        complex_count = len(complexes)
        if not complexes:
            logger.warning("No complexes found for query '%s'", query)
        
        for complex_info in complexes:
            snapshot = crawler.fetch_complex_snapshot(complex_info["id"])
            apt_name = snapshot.get("danjis", {}).get("name") or complex_info["name"]
            room_type_count += store_room_type_prices(conn, apt_name, snapshot)
            price_points += store_price_history(conn, apt_name, snapshot)
            new_listings += store_listings(conn, apt_name, snapshot)
            conn.commit()
            if CRAWL_DELAY > 0:
                time.sleep(CRAWL_DELAY)
        
        # 2. Fetch MOLIT data
        logger.info("Fetching MOLIT data for region: %s", query)
        molit_data = fetch_molit_data(region_name=query)
        if molit_data:
            molit_points = store_molit_data(conn, molit_data)
            price_points += molit_points
            conn.commit()

        reset_new_listing_flags(conn)
        record_crawl_result(conn, "success", new_listings, room_type_count, price_points, complex_count)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Crawl job failed")
        record_crawl_result(conn, "error", new_listings, room_type_count, price_points, complex_count, str(exc))
        conn.commit()
        conn.close()
        raise
    conn.commit()
    conn.close()
    check_alerts()
    return new_listings


def check_alerts() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM price_alerts WHERE active = 1")
    alerts = cur.fetchall()
    if not alerts:
        conn.close()
        return

    one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat(sep=" ", timespec="seconds")
    for alert in alerts:
        (
            _alert_id,
            apt_complex,
            area_type,
            max_price,
            min_area,
            max_area,
            notify_new,
            _active,
            _created_at,
        ) = alert
        query = """
            SELECT apt_complex, price, price_type, area, area_type, monthly_rent, collected_at
            FROM listings
            WHERE datetime(collected_at) >= datetime(?)
        """
        params: List[Any] = [one_hour_ago]
        if apt_complex:
            query += " AND apt_complex = ?"
            params.append(apt_complex)
        if max_price:
            query += " AND price <= ?"
            params.append(max_price)
        if min_area and max_area:
            query += " AND area BETWEEN ? AND ?"
            params.extend([min_area, max_area])
        cur.execute(query, params)
        rows = cur.fetchall()
        if rows and notify_new:
            print("\n🔔 알림 발생!")
            for row in rows[:3]:
                deposit = price_to_uk(row["price"])
                monthly = row["monthly_rent"]
                monthly_text = f"/ {monthly:,}만" if monthly else ""
                print(f"   - {row['apt_complex']} / {deposit}{monthly_text} / {row['area_type']}")
    conn.close()


# ===== 대시보드 데이터 가공 =====
def format_price_range(min_value: Optional[int], max_value: Optional[int]) -> str:
    if min_value is None and max_value is None:
        return "-"
    if min_value is not None and max_value is not None and min_value != max_value:
        return f"{price_to_uk(min_value)} ~ {price_to_uk(max_value)}"
    value = min_value if min_value is not None else max_value
    return price_to_uk(value)


def prepare_dashboard_data() -> Dict[str, Any]:
    conn = get_db_connection()
    cur = conn.cursor()

    total_listings = cur.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    new_listings = cur.execute(
        "SELECT COUNT(*) FROM listings WHERE datetime(collected_at) >= datetime('now', '-1 day')"
    ).fetchone()[0]

    avg_price_display = "-"
    cur.execute("SELECT MAX(captured_date) FROM room_type_prices")
    latest_capture = cur.fetchone()[0]
    if latest_capture:
        avg_price = cur.execute(
            "SELECT AVG(sales_min) FROM room_type_prices WHERE captured_date = ? AND sales_min IS NOT NULL",
            (latest_capture,),
        ).fetchone()[0]
        if avg_price:
            avg_price_display = f"{float(avg_price) / 10000:.1f}억"

    alert_count = cur.execute("SELECT COUNT(*) FROM price_alerts WHERE active = 1").fetchone()[0]
    last_update_row = cur.execute(
        "SELECT crawled_at FROM crawl_log ORDER BY crawled_at DESC LIMIT 1"
    ).fetchone()
    last_update = datetime.now().strftime("%m/%d %H:%M")
    if last_update_row and last_update_row[0]:
        try:
            last_update = datetime.fromisoformat(last_update_row[0]).strftime("%m/%d %H:%M")
        except ValueError:
            last_update = last_update_row[0]

    room_type_rows = cur.execute(
        """
        SELECT apt_complex, complex_id, room_type_name, area_m2, area_pyeong,
               sales_min, sales_max, rent_min, rent_max, source_url
        FROM room_type_prices
        WHERE captured_date = (SELECT MAX(captured_date) FROM room_type_prices)
        ORDER BY apt_complex, area_pyeong
        """
    ).fetchall()

    room_type_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in room_type_rows:
        area_m2_value = safe_float(row["area_m2"])
        area_pyeong_value = safe_float(row["area_pyeong"])
        sales_min = safe_int(row["sales_min"])
        sales_max = safe_int(row["sales_max"])
        rent_min = safe_int(row["rent_min"])
        rent_max = safe_int(row["rent_max"])
        representative_price = compute_representative_price(sales_min, sales_max)
        loan_info = loan_summary_from_price(representative_price)
        room_type_map[row["apt_complex"]].append(
            {
                "complex_id": row["complex_id"],
                "name": row["room_type_name"] or f"{area_pyeong_value or 0:.1f}평",
                "area_m2": area_m2_value,
                "area_pyeong": area_pyeong_value,
                "sales_min": sales_min,
                "sales_max": sales_max,
                "rent_min": rent_min,
                "rent_max": rent_max,
                "source_url": row["source_url"],
                "representative_price": representative_price,
                "loan_amount_display": loan_info["loan_amount"],
                "monthly_payment_display": loan_info["monthly_payment"],
                "loan_amount_man": loan_info["loan_amount_man"],
                "match_keys": build_room_type_match_keys(row["room_type_name"], area_m2_value, area_pyeong_value),
            }
        )

    history_rows = cur.execute(
        """
        SELECT apt_complex, series_label, date, price
        FROM price_history
        WHERE date >= strftime('%Y-%m-%d', date('now', '-7 years'))
        ORDER BY apt_complex, series_label, date
        """
    ).fetchall()

    history_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in history_rows:
        history_map[row["apt_complex"]][row["series_label"]].append(
            {"date": row["date"], "price": safe_int(row["price"])}
        )

    listing_rows = cur.execute(
        """
        SELECT listing_id, apt_complex, price, price_type, area, area_type, floor,
               direction, listing_date, source, url, monthly_rent, collected_at,
               listing_tran_type, area_ho_id
        FROM listings
        ORDER BY apt_complex, datetime(collected_at) DESC
        """
    ).fetchall()

    listing_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in listing_rows:
        price_man = safe_int(row["price"])
        area_value = safe_float(row["area"])
        matched_room_type = find_room_type_match(
            room_type_map.get(row["apt_complex"], []), row["area_type"], area_value
        )
        loan_amount_display: Optional[str] = None
        monthly_payment_display: Optional[str] = None
        loan_amount_man: Optional[int] = None
        if matched_room_type:
            loan_amount_display = matched_room_type.get("loan_amount_display")
            monthly_payment_display = matched_room_type.get("monthly_payment_display")
            loan_amount_man = matched_room_type.get("loan_amount_man")
            if (
                not loan_amount_display
                or not monthly_payment_display
                or loan_amount_man is None
            ):
                fallback_info = loan_summary_from_price(matched_room_type.get("representative_price"))
                loan_amount_display = fallback_info["loan_amount"]
                monthly_payment_display = fallback_info["monthly_payment"]
                loan_amount_man = fallback_info["loan_amount_man"]
        else:
            fallback_info = loan_summary_from_price(price_man)
            loan_amount_display = fallback_info["loan_amount"]
            monthly_payment_display = fallback_info["monthly_payment"]
            loan_amount_man = fallback_info["loan_amount_man"]
        
        needed_cash_display = "-"
        needed_cash_man = None
        if price_man is not None and loan_amount_man is not None:
            needed_cash_man = max(int(round(price_man - loan_amount_man)), 0)
            needed_cash_display = price_to_uk(needed_cash_man)
        
        listing_id = row["listing_id"]
        mobile_url = row["url"]
        tran_type_value = row["listing_tran_type"] or "trade"
        area_ho_id = row["area_ho_id"]
        catalog_url = None
        if area_ho_id:
            catalog_url = (
                f"https://www.zigbang.com/home/apt/item-catalog?areaHoId={area_ho_id}&tranType={tran_type_value}"
            )
        web_url = catalog_url or f"https://www.zigbang.com/home/search?type=apt&q={quote_plus(row['apt_complex'])}"
        listing_map[row["apt_complex"]].append(
            {
                "listing_id": listing_id,
                "price": price_to_uk(row["price"]),
                "price_type": row["price_type"],
                "monthly_rent": f"{row['monthly_rent']:,}만" if row["monthly_rent"] else "",
                "area": row["area"],
                "area_type": row["area_type"],
                "floor": row["floor"],
                "source": row["source"],
                "url": mobile_url,
                "web_url": web_url,
                "catalog_url": catalog_url,
                "collected_at": row["collected_at"],
                "loan_amount": loan_amount_display,
                "monthly_payment": monthly_payment_display,
                "needed_cash": needed_cash_display,
                "needed_cash_raw": needed_cash_man,
            }
        )

    conn.close()

    complex_names = sorted(
        set(room_type_map.keys()) | set(history_map.keys()) | set(listing_map.keys())
    )

    complex_cards: List[Dict[str, Any]] = []
    sparkline_payload: Dict[str, Dict[str, Any]] = {}
    for name in complex_names:
        room_types = room_type_map.get(name, [])
        complex_history_map = history_map.get(name, {})
        listings = listing_map.get(name, [])[:5]
        complex_source_url = None

        # Default history is for the complex itself
        history = complex_history_map.get(name, [])
        history_tail = [point for point in history if point["price"] is not None]

        latest_chart_value: Optional[int] = history_tail[-1]["price"] if history_tail else None
        previous_value: Optional[int] = None
        if history_tail:
            # Calculate previous value from 6 months ago if enough data
            previous_index = max(0, len(history_tail) - 6)
            previous_value = history_tail[previous_index]["price"]
        elif room_types:
            latest_chart_value = room_types[0].get("sales_min")

        change_value: Optional[int] = None
        change_percent: Optional[float] = None
        if latest_chart_value is not None and previous_value not in (None, 0):
            change_value = latest_chart_value - previous_value
            if previous_value:
                change_percent = (change_value / previous_value) * 100

        room_type_entries: List[Dict[str, Any]] = []
        for rt in room_types:
            area_summary = "-"
            if rt["area_m2"] and rt["area_pyeong"]:
                area_summary = f"{rt['area_m2']:.1f}㎡ / {rt['area_pyeong']:.1f}평"
            elif rt["area_m2"]:
                area_summary = f"{rt['area_m2']:.1f}㎡"
            elif rt["area_pyeong"]:
                area_summary = f"{rt['area_pyeong']:.1f}평"

            loan_amount_display = rt.get("loan_amount_display")
            monthly_payment_display = rt.get("monthly_payment_display")
            if not loan_amount_display or not monthly_payment_display:
                fallback_info = loan_summary_from_price(rt.get("sales_min") or rt.get("sales_max"))
                loan_amount_display = fallback_info["loan_amount"]
                monthly_payment_display = fallback_info["monthly_payment"]
            if rt.get("source_url") and not complex_source_url:
                complex_source_url = rt["source_url"]

            room_type_entries.append(
                {
                    "name": rt["name"],
                    "area": area_summary,
                    "sales": format_price_range(rt["sales_min"], rt["sales_max"]),
                    "rent": format_price_range(rt["rent_min"], rt["rent_max"]),
                    "loan_amount": loan_amount_display,
                    "monthly_payment": monthly_payment_display,
                    "source_url": rt.get("source_url"),
                }
            )

        if not complex_source_url and listings:
            candidate = listings[0].get("catalog_url") or listings[0].get("web_url")
            if candidate:
                complex_source_url = candidate
        if not complex_source_url:
            complex_source_url = f"https://www.zigbang.com/home/search?type=apt&q={quote_plus(name)}"
        for entry in room_type_entries:
            if not entry["source_url"]:
                entry["source_url"] = complex_source_url

        sparkline_payload[name] = {}
        # Add complex-level history (default)
        sparkline_payload[name][name] = {
            "labels": [point["date"] for point in history_tail],
            "prices": [point["price"] for point in history_tail],
        }
        # Add MOLIT history if available
        molit_history = complex_history_map.get("실거래", [])
        molit_history_tail = [p for p in molit_history if p["price"] is not None]
        if molit_history_tail:
            sparkline_payload[name]["실거래"] = {
                "labels": [p["date"] for p in molit_history_tail],
                "prices": [p["price"] for p in molit_history_tail],
            }

        # Add room type-level history, ensuring keys match rt.name
        for rt in room_types:
            rt_name = rt["name"]
            rt_history = complex_history_map.get(rt_name, [])
            rt_history_tail = [p for p in rt_history if p["price"] is not None]
            sparkline_payload[name][rt_name] = {
                "labels": [p["date"] for p in rt_history_tail],
                "prices": [p["price"] for p in rt_history_tail],
            }

        complex_cards.append(
            {
                "name": name,
                "latest_sale": f"{latest_chart_value:,} (만)" if latest_chart_value else "-",
                "change_text": format_change(change_value, change_percent),
                "change_direction": (
                    "up" if change_value and change_value > 0 else "down" if change_value and change_value < 0 else "flat"
                ),
                "room_types": room_type_entries,
                "listings": listings,
                "source_url": complex_source_url,
            }
        )

    stats = {
        "total_listings": total_listings,
        "new_listings": new_listings,
        "avg_price": avg_price_display,
        "alert_count": alert_count,
        "last_update": last_update,
    }

    return {
        "stats": stats,
        "complex_cards": complex_cards,
        "sparkline": sparkline_payload,
    }


# ===== Flask 앱 =====
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["JSON_AS_ASCII"] = False

scheduler = BackgroundScheduler(daemon=True)


@app.route("/exports/<path:filename>")
def download_export(filename: str):
    """Serve locally cached export files for download."""

    return send_from_directory(EXPORT_DIR, filename, as_attachment=True)


def calculate_loan_payment(amount_man: float, years: int, interest_rate: float) -> Dict[str, str]:
    """월 상환액 계산 (원리금 균등)."""
    principal = amount_man * 10000
    if principal <= 0:
        return {"monthly_payment_display": "0만/월"}
    
    monthly_rate = (interest_rate / 100) / 12
    total_months = years * 12
    
    if monthly_rate == 0:
        payment = principal / total_months
    else:
        payment = principal * (monthly_rate * (1 + monthly_rate) ** total_months) / (
            (1 + monthly_rate) ** total_months - 1
        )
        
    return {
        "monthly_payment_display": f"{payment/10000:,.0f}만/월"
    }


@app.route("/")
def index():
    query = request.args.get("q")
    display_query = query or TARGET_QUERY

    if query:
        logger.info(">>> Frontend search triggered for query: '%s'", query)
        try:
            # When searching, clear old listings to avoid mixing data
            conn = get_db_connection()
            conn.execute("DELETE FROM listings")
            conn.execute("DELETE FROM room_type_prices")
            conn.execute("DELETE FROM price_history")
            conn.commit()
            conn.close()
            crawl_job(query=query)
        except Exception as exc:
            logger.exception("Search-triggered crawl for '%s' failed", query)

    dashboard = prepare_dashboard_data()
    return render_template(
        "index.html",
        stats=dashboard["stats"],
        complexes=dashboard["complex_cards"],
        sparkline_json=json.dumps(dashboard["sparkline"], ensure_ascii=False),
        LOAN_LTV=LOAN_LTV,
        LOAN_INTEREST=LOAN_INTEREST,
        LOAN_YEARS=LOAN_YEARS,
        current_query=display_query,
    )


@app.route("/regional")
def regional_view():
    """Render a page with MOLIT regional transaction data."""
    region_options: List[Dict[str, str]] = []
    seen_codes: Set[str] = set()
    for name, code in LAWD_CODE_MAP.items():
        if code in seen_codes:
            continue
        region_options.append({"name": name, "code": code})
        seen_codes.add(code)
    region_options.sort(key=lambda item: item["name"])

    if not region_options:
        region_options.append({"name": "분당구", "code": "41135"})

    default_region = region_options[0]
    region_param = request.args.get("region")
    month_param = request.args.get("month")
    keyword = (request.args.get("keyword") or "").strip()
    history_region = request.args.get("history_region")
    pyeong_min = request.args.get("pyeong_min", type=float)
    pyeong_max = request.args.get("pyeong_max", type=float)
    history_requested = request.args.get("history") == "1"

    history_csv: Optional[str] = None
    history_chart: List[Dict[str, Any]] = []
    history_error: Optional[str] = None

    selected_region_name = default_region["name"]
    lawd_code = default_region["code"]

    if region_param:
        if region_param in LAWD_CODE_MAP:
            selected_region_name = region_param
            lawd_code = LAWD_CODE_MAP[region_param]
        else:
            matched = next((opt for opt in region_options if opt["code"] == region_param), None)
            if matched:
                selected_region_name = matched["name"]
                lawd_code = matched["code"]
            elif region_param.isdigit():
                lawd_code = region_param
                selected_region_name = region_param

    today = datetime.now()
    default_month = today.strftime("%Y-%m")
    month_input = month_param or default_month
    deal_ym = re.sub(r"[^0-9]", "", month_input)[:6]
    if len(deal_ym) != 6:
        deal_ym = today.strftime("%Y%m")
        month_input = default_month

    transactions: List[Dict[str, Any]] = []
    error_message: Optional[str] = None
    if not DATA_API_KEY:
        error_message = "DATA_API_KEY 환경 변수가 설정되어야 실거래가 데이터를 조회할 수 있습니다."
    elif not lawd_code:
        error_message = "지원하지 않는 지역입니다."
    else:
        try:
            transactions = fetch_molit_transactions_by_code(lawd_code, deal_ym)
        except (requests.RequestException, ET.ParseError, RuntimeError) as exc:
            logger.exception("Failed to fetch MOLIT regional data")
            error_message = f"실거래가 데이터를 불러오는 중 오류가 발생했습니다: {exc}"

    if keyword and transactions:
        keyword_lower = keyword.lower()
        transactions = [t for t in transactions if keyword_lower in t["apartment"].lower()]

    if history_requested:
        target_region_code = history_region or lawd_code
        if not target_region_code:
            history_error = "CSV를 생성하려면 지역을 선택해야 합니다."
        elif pyeong_min is None or pyeong_max is None:
            history_error = "평형 범위를 입력해주세요."
        else:
            try:
                history_csv, history_chart = export_molit_history_csv(
                    target_region_code, pyeong_min, pyeong_max, years=7
                )
                if not history_csv:
                    history_error = "조건에 맞는 7개년 실거래 데이터가 없습니다."
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Failed to export MOLIT CSV")
                history_error = f"CSV 생성 중 오류가 발생했습니다: {exc}"

    prices = [t["price"] for t in transactions if t.get("price")]
    summary = {
        "count": len(transactions),
        "avg_price_display": "-",
        "max_record": None,
        "min_record": None,
    }
    if prices:
        avg_price = int(sum(prices) / len(prices))
        summary["avg_price_display"] = format_trade_price(avg_price)
        max_record = max((t for t in transactions if t.get("price")), key=lambda item: item["price"])
        min_record = min((t for t in transactions if t.get("price")), key=lambda item: item["price"])
        summary["max_record"] = {
            "apartment": max_record["apartment"],
            "price": max_record["price"],
            "price_display": format_trade_price(max_record["price"]),
        }
        summary["min_record"] = {
            "apartment": min_record["apartment"],
            "price": min_record["price"],
            "price_display": format_trade_price(min_record["price"]),
        }

    return render_template(
        "regional.html",
        region_options=region_options,
        selected_region=selected_region_name,
        selected_region_code=lawd_code,
        month_input=month_input,
        transactions=transactions,
        keyword=keyword,
        summary=summary,
        error_message=error_message,
        history_error=history_error,
        history_csv=history_csv,
        history_chart=history_chart,
        pyeong_min=pyeong_min,
        pyeong_max=pyeong_max,
        history_region_code=history_region or lawd_code,
    )


@app.route("/api/calculate-loan", methods=["POST"])
def api_calculate_loan():
    data = request.json or {}
    amount_man = data.get("amount_man")
    years = data.get("years")
    interest_rate = data.get("interest_rate")

    if not all([amount_man, years, interest_rate]):
        return jsonify({"error": "모든 값을 입력해주세요."}), 400
    
    try:
        result = calculate_loan_payment(float(amount_man), int(years), float(interest_rate))
        return jsonify(result)
    except (ValueError, TypeError):
        return jsonify({"error": "잘못된 숫자 형식입니다."}), 400
    except Exception as exc:
        logger.exception("Loan calculation failed")
        return jsonify({"error": f"계산 중 오류 발생: {exc}"}), 500


@app.route("/api/crawl")
def api_crawl():
    query = request.args.get("q", default=TARGET_QUERY, type=str)
    try:
        new_entries = crawl_job(query=query)
        message = f"'{query}' 지역 크롤링이 완료되었습니다 (신규/변경 {new_entries}건)"
        return jsonify({"message": message})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"message": f"오류: {exc}"}), 500


@app.route("/api/alert/add", methods=["POST"])
def api_add_alert():
    data = request.json or {}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO price_alerts (apt_complex, area_type, max_price, min_area, max_area)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                data.get("complex"),
                data.get("areaType"),
                data.get("maxPrice"),
                data.get("minArea"),
                data.get("maxArea"),
            ),
        )
        conn.commit()
        return jsonify({"message": "알림이 설정되었습니다"})
    except Exception as exc:  # pylint: disable=broad-except
        conn.rollback()
        return jsonify({"message": f"오류: {exc}"}), 500
    finally:
        conn.close()


@app.route("/api/export")
def api_export():
    conn = get_db_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT apt_complex, room_type_name, area_m2, area_pyeong,
               sales_min, sales_max, rent_min, rent_max, captured_date
        FROM room_type_prices
        ORDER BY apt_complex, room_type_name, captured_date
        """
    ).fetchall()
    conn.close()

    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        ["단지명", "타입", "전용면적(㎡)", "전용면적(평)", "매매가 최소", "매매가 최대", "전월세 최소", "전월세 최대", "기준일"]
    )
    for row in rows:
        writer.writerow(
            [
                row["apt_complex"],
                row["room_type_name"],
                f"{safe_float(row['area_m2']) or 0:.1f}" if row["area_m2"] else "",
                f"{safe_float(row['area_pyeong']) or 0:.1f}" if row["area_pyeong"] else "",
                row["sales_min"],
                row["sales_max"],
                row["rent_min"],
                row["rent_max"],
                row["captured_date"],
            ]
        )

    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=yatap_market_snapshot.csv"
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    return response


def start_scheduler() -> None:
    if not scheduler.running:
        scheduler.add_job(crawl_job, "interval", hours=2, id="crawl_job", max_instances=1, args=[TARGET_QUERY])
        scheduler.start()
        logger.info("Background scheduler started for '%s' (2 hour interval)", TARGET_QUERY)


if __name__ == "__main__":
    init_db()
    start_scheduler()
    try:
        logger.info("🔧 Initial crawl to warm up data for '%s'", TARGET_QUERY)
        crawl_job(query=TARGET_QUERY)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Initial crawl failed: %s", exc)
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
