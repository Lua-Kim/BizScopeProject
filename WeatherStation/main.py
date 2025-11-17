import os
import io
import requests
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime


def read_parquet_from_adls(connection_string, container_name, blob_path):
    """
    Azure Data Lake Storage에서 Parquet 파일을 읽어 Pandas DataFrame으로 반환합니다.
    
    :param connection_string: Azure Storage 연결 문자열
    :param container_name: 컨테이너 이름
    :param blob_path: Parquet 파일의 Blob 경로
    :return: Pandas DataFrame
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        blob_client = container_client.get_blob_client(blob_path)
        download_stream = blob_client.download_blob()
        parquet_bytes = download_stream.readall()
        
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        
        print("✅ ADLS에서 Parquet 데이터 읽기 완료! 미리보기:")
        print(df.head())
        
        output_path = "region_lookup_preview.csv"
        df.to_csv(output_path, index=False)
        print(f"\n💾 CSV 파일로 저장 완료: {output_path}")
        
        return df
    except Exception as e:
        print(f"❌ ADLS에서 데이터 읽기 오류: {e}")
        return None

def upload_csv_to_adls(connection_string, container_name, local_file_path, blob_path):
    """
    로컬 CSV 파일을 Azure Data Lake Storage에 업로드합니다.

    :param connection_string: Azure Storage 연결 문자열
    :param container_name: 컨테이너 이름
    :param local_file_path: 업로드할 로컬 CSV 파일 경로
    :param blob_path: ADLS에 저장될 Blob 경로
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_path)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"✅ 로컬 파일 '{local_file_path}'을 ADLS '{container_name}/{blob_path}'에 성공적으로 업로드했습니다.")
    except Exception as e:
        print(f"❌ ADLS에 파일 업로드 오류: {e}")

def parse_weather_text(text):
    """
    기상청 API 응답 텍스트를 파싱하여 DataFrame으로 변환합니다.
    """
    lines = [line for line in text.splitlines() if line.strip() and not line.startswith("#")]
    rows = []

    for line in lines:
        parts = line.split()
        # 앞의 10개 숫자 필드
        fixed = parts[:10]
        # 뒤의 3개 코드 필드
        tail = parts[-3:]
        # 중간에 남은 것들: 한글 지점명 + 영문 지점명
        middle = parts[10:-3]

        if len(middle) >= 2:
            stn_ko = middle[0]
            stn_en = " ".join(middle[1:])  # 영문 지점명은 공백 포함 가능
        else:
            stn_ko = middle[0]
            stn_en = ""

        row = fixed + [stn_ko, stn_en] + tail
        rows.append(row)

    headers = [
        "STN_ID","LON","LAT","STN_SP","HT","HT_PA","HT_TA",
        "HT_WD","HT_RN","STN_CD","STN_KO","STN_EN","FCT_ID","LAW_ID","BASIN"
    ]
    df = pd.DataFrame(rows, columns=headers)
    return df


def get_weather_data(auth_key, timestamp):
    """
    기상청 API에서 특정 시점의 기상 관측 자료를 가져옵니다.
    """
    print(f"\n🔹 기상청 API에서 데이터 가져오는 중... (시각: {timestamp})")

    api_endpoint = f"https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=SFC&stn=0&tm={timestamp}"
    api_url = f"{api_endpoint}&help=0&authKey={auth_key}"

    try:
        response = requests.get(api_url)
        response.raise_for_status()

        # 주석(#) 제거
        content = response.text
        lines = [line for line in content.split('\n') if not line.strip().startswith('#')]
        csv_content = '\n'.join(lines)

        if not csv_content.strip():
            print("⚠️ API 응답에 데이터가 없습니다.")
            return None

        # ✅ 파싱 함수 호출
        weather_df = parse_weather_text(csv_content)

        print("✅ API 응답 파싱 완료! 데이터 미리보기:")
        print(weather_df.head())

        return weather_df

    except requests.exceptions.RequestException as e:
        print(f"❌ API 호출 오류: {e}")
        return None
    except Exception as e:
        print(f"❌ 데이터 파싱 오류: {e}")
        return None

def get_access_token(consumer_key: str, consumer_secret: str) -> str:
    """
    SGIS API AccessToken 발급 함수
    """
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    params = {
        "consumer_key": consumer_key,
        "consumer_secret": consumer_secret
    }
    res = requests.get(url, params=params)
    res.raise_for_status()
    data = res.json()
    return data["result"]["accessToken"]


def reverse_geocode(access_token: str, lon: float, lat: float, addr_type: int = 20) -> dict:
    """
    SGIS 리버스 지오코딩 (좌표 → 주소)
    addr_type: 10=지번주소, 20=도로명주소
    """
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/rgeocodewgs84.json"
    params = {
        "accessToken": access_token,
        "x_coor": lon,
        "y_coor": lat,
        "addr_type": addr_type
    }
    res = requests.get(url, params=params)
    res.raise_for_status()
    print(res.json())
    return res.json()


def reverse_geocode_df(access_token: str, lon: float, lat: float, addr_type: int = 20) -> pd.DataFrame:
    """
    SGIS 리버스 지오코딩 결과를 DataFrame으로 반환
    """
    rgeo_json = reverse_geocode(access_token, lon, lat, addr_type)

    if "result" in rgeo_json and isinstance(rgeo_json["result"], list):
        return pd.DataFrame(rgeo_json["result"])
    else:
        return pd.DataFrame()  # 실패 시 빈 DF



def geocode(access_token: str, address: str, pagenum: int = 0, resultcount: int = 5) -> dict:
    """
    SGIS 지오코딩 (주소 → 좌표)
    """
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocode.json"
    params = {
        "accessToken": access_token,
        "address": address,
        "pagenum": pagenum,
        "resultcount": resultcount
    }
    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json()


def sgis_to_dataframe(json_data: dict) -> pd.DataFrame:
    """
    SGIS API 응답(JSON)을 판다스 DataFrame으로 변환
    - geocode 응답: result.resultdata 리스트를 DataFrame으로 변환
    - reverse geocode 응답: result 딕셔너리를 DataFrame으로 변환
    """
    if "result" not in json_data:
        return pd.DataFrame()

    result = json_data["result"]

    # geocode 응답 (좌표 리스트)
    if "resultdata" in result:
        return pd.DataFrame(result["resultdata"])

    # reverse geocode 응답 (주소 정보)
    else:
        return pd.DataFrame([result])



def enrich_weather_data(weather_df, access_token):
    """
    기후 관측소 데이터에 리버스 지오코딩 결과를 추가
    - sido_nm → 도/광역시 이름
    - sgg_nm → 시/군/구 이름 (있으면 구까지 포함, 없으면 시/군만)
    - emdong_nm → 읍/면/동 이름 (없으면 빈칸)
    - full_addr → 전체 주소
    """
    for col in ["도", "시군구", "읍면동", "전체주소"]:
        weather_df[col] = ""

    for idx, row in weather_df.iterrows():
        lon, lat = row["LON"], row["LAT"]

        # 리버스 지오코딩 호출 → DataFrame 반환
        rgeo_df = reverse_geocode_df(access_token, lon, lat, addr_type=20)

        if not rgeo_df.empty:
            sido_nm   = rgeo_df.loc[0, "sido_nm"]   if "sido_nm"   in rgeo_df.columns else ""
            sgg_nm    = rgeo_df.loc[0, "sgg_nm"]    if "sgg_nm"    in rgeo_df.columns else ""
            emdong_nm = rgeo_df.loc[0, "emdong_nm"] if "emdong_nm" in rgeo_df.columns else ""
            full_addr = rgeo_df.loc[0, "full_addr"] if "full_addr" in rgeo_df.columns else ""

            # weather_df에 값 추가
            weather_df.at[idx, "도"] = sido_nm
            weather_df.at[idx, "시군구"] = sgg_nm if sgg_nm else ""
            weather_df.at[idx, "읍면동"] = emdong_nm if emdong_nm else ""
            weather_df.at[idx, "전체주소"] = full_addr

    return weather_df




def main():
    # .env 파일에서 환경 변수 로드
    load_dotenv()

    # 1. AccessToken 발급
    token = get_access_token("433f72dd0d464fab94d1", "bde93ac5b0e6428c84ee")

    # 환경 변수 로드 및 검증
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    kma_auth_key = os.getenv('KMA_API_AUTH_KEY')
    blob_Gold_path = "gold/lookup_tables/region_lookup/region_lookup.parquet"
    blob_Silver_path = "silver/"

    if not all([connection_string, container_name, kma_auth_key]):
        print("❌ 필수 환경 변수가 설정되지 않았습니다. (.env 파일을 확인하세요)")
        print("   - AZURE_STORAGE_CONNECTION_STRING")
        print("   - AZURE_CONTAINER_NAME")
        print("   - KMA_API_AUTH_KEY")
        return
    
    # ADLS에서 Parquet 데이터 읽기
    # print("🔹 Azure Data Lake에서 Parquet 데이터 읽는 중...")
    # region_df = read_parquet_from_adls(connection_string, container_name, blob_Gold_path)

    # if region_df is not None:
    #     print("\n📊 지역 데이터 요약:")
    #     region_df.info()
    # else:
    #     print("⚠️ 지역 데이터를 불러오지 못했습니다.")

    # 기상 API 데이터 가져오기
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M')
    weather_df = get_weather_data(kma_auth_key, current_timestamp)

    if weather_df is not None:
        enriched_df = enrich_weather_data(weather_df, token)
        print("\n🌦️ 날씨 관측소 데이터 요약:")
        enriched_df.info()

        output_filename = f"enriched_weather_stations_{current_timestamp}.csv"
        enriched_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\n💾 위치와 주소가 추가된 날씨 관측소 데이터 CSV 파일로 저장 완료: {output_filename}")
        
        upload_csv_to_adls(connection_string, container_name, output_filename, blob_Silver_path)
    else:
        print("⚠️ 날씨 관측소 데이터를 불러오지 못했습니다.")

if __name__ == "__main__":
    main()