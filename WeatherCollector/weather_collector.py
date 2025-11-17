import os
import calendar
import requests
import pandas as pd
import io
import time
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# 56개 컬럼명
COLUMNS = [
    "TM", "STN", "WS_AVG", "WR_DAY", "WD_MAX", "WS_MAX", "WS_MAX_TM", "WD_INS", "WS_INS", "WS_INS_TM",
    "TA_AVG", "TA_MAX", "TA_MAX_TM", "TA_MIN", "TA_MIN_TM", "TD_AVG", "TS_AVG", "TG_MIN",
    "HM_AVG", "HM_MIN", "HM_MIN_TM", "PV_AVG", "EV_S", "EV_L", "FG_DUR", "PA_AVG", "PS_AVG",
    "PS_MAX", "PS_MAX_TM", "PS_MIN", "PS_MIN_TM", "CA_TOT", "SS_DAY", "SS_DUR", "SS_CMB",
    "SI_DAY", "SI_60M_MAX", "SI_60M_MAX_TM", "RN_DAY", "RN_D99", "RN_DUR", "RN_60M_MAX",
    "RN_60M_MAX_TM", "RN_10M_MAX", "RN_10M_MAX_TM", "RN_POW_MAX", "RN_POW_MAX_TM",
    "SD_NEW", "SD_NEW_TM", "SD_MAX", "SD_MAX_TM", "TE_05", "TE_10", "TE_15", "TE_30", "TE_50"
]

# .env에서 로드
load_dotenv()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)


def parse_weather_text(text_data: str) -> pd.DataFrame:
    """
    월별 텍스트 데이터를 받아 DataFrame으로 변환합니다.
    주석(#)과 설명 라인은 제거하고 실제 데이터만 변환합니다.
    """
    # 텍스트를 줄 단위로 분리
    lines = text_data.splitlines()
    
    # 주석(#) 제거
    data_lines = [line for line in lines if line.strip() and not line.startswith("#")]
    
    # 공백 여러개를 단일 공백으로 변경
    data_lines = [" ".join(line.split()) for line in data_lines]
    
    # 문자열을 CSV처럼 읽어서 DataFrame 생성
    csv_like = "\n".join(data_lines)
    df = pd.read_csv(io.StringIO(csv_like), sep="\s+", header=None, names=COLUMNS, engine='python')
    
    return df

# .env 파일에서 환경 변수 로드
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"

def get_weather_data_monthly(year, month, location_code=0):
    """
    기상청 API를 호출하여 지정된 연도와 월, 지역의 월별 기상 데이터를 텍스트 형태로 가져옵니다.
    """
    if not API_KEY:
        raise ValueError("환경변수 'API_KEY'가 설정되어 있지 않습니다.")

    start_date = f"{year}{month:02d}01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{last_day}"

    params = {
        'tm1': start_date,
        'tm2': end_date,
        'stn': location_code,
        'help': "0",  # 주석 없이 순수 데이터만 받음
        'authKey': API_KEY,
    }

    for attempt in range(3):  # 3번 재시도
        try:
            response = requests.get(BASE_URL, params=params, timeout=100)
            response.raise_for_status()
            text_data = response.text
            print(f"{start_date} ~ {end_date} : 데이터 가져오기 성공")
            return text_data
        except requests.exceptions.RequestException as e:
            print(f"{start_date} ~ {end_date} : API 요청 오류 (시도 {attempt + 1}/3) - {e}")
            if attempt < 2:  # 마지막 시도가 아니면 대기
                time.sleep(5)  # 5초 대기
    print(f"{start_date} ~ {end_date} : 3번의 시도에도 불구하고 데이터 가져오기 실패.")
    return None

def save_data_to_csv(dataframe, start_date, location_code):
    """
    데이터프레임을 CSV 파일로 저장합니다. 파일이 이미 존재하면 저장하지 않고 경로를 반환합니다.
    """
    if dataframe is None:
        print("저장할 데이터가 없습니다.")
        return None

    if not os.path.exists('data'):
        os.makedirs('data')

    filename = f"data/{start_date}-{location_code}.csv"
    
    if os.path.exists(filename):
        print(f"'{filename}' 파일이 이미 존재합니다. 저장을 건너뜁니다.")
        return filename

    try:
        dataframe.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"데이터를 '{filename}' 파일에 저장했습니다.")
        return filename
    except Exception as e:
        print(f"파일 저장 중 오류가 발생했습니다: {e}")
        return None


def upload_to_data_lake(file_path, blob_name):
    """
    CSV 파일을 Azure Data Lake 컨테이너에 업로드합니다.
    """
    if not os.path.exists(file_path):
        print(f"{file_path} 파일이 존재하지 않습니다.")
        return


    with open(file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"{blob_name} 업로드 완료")



if __name__ == "__main__":
    print("기상 데이터 수집을 시작합니다.")

    location_code = 0  # 전체 관측소
    for year in range(2022, 2026):
        start_month = 1
        end_month = 12
        if year == 2025:
            end_month = 10  # 2025년은 10월까지만

        for month in range(start_month, end_month + 1):
            monthly_data = get_weather_data_monthly(year, month, location_code)
            if monthly_data:
                # 주석 제거하고 컬럼 적용
                weather_df = parse_weather_text(monthly_data)
                
                # 첫 번째 행만 확인용 출력
                first_row = weather_df.iloc[0]
                print(first_row.tolist())
                print(f"총 항목 수: {len(first_row)}")  # 56이어야 함

                # CSV 파일 저장
                start_dt = f"Weather_{year}{month:02d}01"
                file_path = save_data_to_csv(weather_df, start_dt, location_code)

                # 데이터 레이크 업로드
                blob_name = f"bronze/Weather/{year}/{start_dt}-{location_code}.csv"
                upload_to_data_lake(file_path, blob_name)

    print("기상 데이터 수집을 종료합니다.")