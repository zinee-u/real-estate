# Real Estate Dashboard

Flask 기반 대시보드로 국토교통부 실거래가 오픈 API를 호출해 지역별 아파트 거래를 보여 줍니다. `/regional` 페이지에서 시·군·구 코드(LAWD_CD)와 조회 연월(DEAL_YMD), 단지 키워드를 조합하여 데이터를 필터링할 수 있습니다. 추가로, 원하는 평형 범위를 선택해 최근 7개년 실거래 데이터를 CSV로 로컬에 저장하고 해당 CSV를 기반으로 그래프를 그릴 수 있습니다.

## 국토교통부 HTTPS 게이트웨이 접근 거부 현상

앱은 기본적으로 `https://openapi.molit.go.kr` 의 443 포트를 먼저 호출한 뒤, 실패하면 `http://openapi.molit.go.kr:8081` 로 자동 폴백합니다. 일부 망에서는 아래 이유로 443 연결이 거부되며 로그에 `Failed to establish a new connection` 메시지가 남습니다.

- **회사/기관 방화벽**: TLS 패킷 검사 정책 때문에 특정 외부 호스트의 443 포트를 차단
- **프록시/게이트웨이 누락**: 내부 네트워크가 HTTPS 트래픽을 허용하지 않거나, 인증서를 강제 삽입하는 장비 때문에 세션이 생성되지 못하는 경우
- **IDC/서버 호스팅 정책**: 서버 보안 정책상 아웃바운드 443 포트가 기본 차단되어 있는 환경

이 경우 애플리케이션 코드 문제와 무관하게 OS 수준에서 소켓이 열리지 않으므로, HTTPS 요청이 항상 실패합니다. 로그에 보이는 `Connection refused` 는 이러한 네트워크 차단을 의미합니다.

## HTTP 포트 8081 경로 여는 방법

애플리케이션은 443 실패 시 `http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/...` 엔드포인트로 재시도합니다. HTTP 경로까지 막혀 있으면 폴백 역시 동작하지 않으므로 다음 조치를 수행하세요.

1. **방화벽 아웃바운드 허용**: `openapi.molit.go.kr` 대상의 TCP 8081 포트를 허용 규칙에 추가합니다. 서버/클라우드 보안 그룹, 사내 방화벽 모두 동일하게 설정해야 합니다.
2. **프록시 화이트리스트**: 조직 프록시를 사용하는 경우 `http://openapi.molit.go.kr:8081` 도메인·포트를 화이트리스트에 추가하여 프록시가 트래픽을 통과시킬 수 있게 합니다.
3. **접근 확인**: 아래 curl 명령으로 응답을 확인합니다(환경 변수 `DATA_API_KEY` 는 실제 서비스 키로 대체).
   ```bash
   curl "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?serviceKey=$DATA_API_KEY&LAWD_CD=41135&DEAL_YMD=202511&numOfRows=1"
   ```
   XML 또는 JSON 응답이 내려오면 8081 경로가 열린 것입니다.

8081 경로가 열려 있으면 애플리케이션이 자동으로 HTTP 게이트웨이로 폴백하므로 별도의 코드 수정 없이 `/regional` 페이지가 데이터를 표시합니다.

## HTTP 8081 연결도 거부될 때

위와 같은 조치 이후에도 `http://openapi.molit.go.kr:8081/...` 요청에서조차 `Connection refused` 메시지가 나온다면 다음 환경적 제약을 의심해야 합니다.

- **서버/클라우드의 아웃바운드 정책**: 일부 호스팅 서비스는 기본적으로 80/443 외 포트를 막습니다. 보안 그룹 또는 네트워크 ACL에 8081 허용 규칙을 추가해야 합니다.
- **조직형 프록시**: HTTP 트래픽도 프록시를 반드시 거쳐야 하는데, `openapi.molit.go.kr:8081` 이 등록되어 있지 않으면 소켓 연결이 즉시 끊깁니다. 프록시 화이트리스트에 추가하거나 `HTTP_PROXY` 환경 변수를 올바르게 지정해야 합니다.
- **로컬 방화벽/보안 제품**: 서버 내부의 iptables, Windows Defender Firewall, AhnLab 같은 보안 제품이 비표준 포트를 차단하는 경우도 있습니다.

이 경우 애플리케이션 로그에는 `MOLIT API 요청 실패(http://openapi.molit.go.kr:8081/...): Failed to establish a new connection` 처럼 마지막으로 시도한 게이트웨이가 함께 찍힙니다. 위 원인을 해소해 8081 포트로 나가는 TCP 세션이 열려야 폴백이 성공합니다. 조치 후에는 다시 한 번 `curl http://openapi.molit.go.kr:8081/...` 명령으로 응답을 확인해 주세요.

## 7개년 CSV 저장 및 그래프 표시

- `/regional` 상단의 "최근 7개년 CSV & 그래프" 카드에서 **CSV 대상 지역**과 **평형 범위(평)** 를 지정한 뒤 `CSV 저장 & 그래프 보기` 버튼을 누르세요.
- 앱이 최근 7개년(84개월) 데이터를 국토부 API에서 내려받아 `exports/` 디렉터리 아래 `molit_<지역코드>_<평형범위>p_<타임스탬프>.csv` 파일로 저장합니다.
- 저장된 CSV를 기반으로 월별 평균 거래가 라인 차트를 그리며, `CSV 다운로드` 버튼으로 로컬에 곧바로 내려받을 수 있습니다.
