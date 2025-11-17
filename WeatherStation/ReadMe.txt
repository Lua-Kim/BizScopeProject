프로젝트 폴더명: WeatherStation
가상청 지상 관측소 API 데이터 만들기.
- 목적 : MS Power BI에서 데이터 분석을 위한 데이터 준비작업
- 기상청 API 허브에서 자상 관측 데이터의 스테이션 위치를 불러오기. 
- 리버스 지오코딩으로 주소 값 붙인다.
    - 경기도 카드 데이터와 서울 인구 데이터, 기후 데이터의 기준값이 달라서 동단위로 엮어서 분석하기 위한 작업
    - 좌표 데이터가 있으나 법정동 코드와 행정도 코드가 호환이 되지 않아 문자열 주소로 호환 가능하게 하기 위함이다. 
    - 협업 및 백업을 위해 먼저 csv로 저장
    - 목적지는 MS ADLS이나.. 바쁘면 수동 업로드 가능
    -가상청 지상 관측소 API 예시
https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=SFC&stn=0&tm=20220101&help=1&authKey=your_key
 
가상환경 : WeatherStation_venv
환경변수 : .venv