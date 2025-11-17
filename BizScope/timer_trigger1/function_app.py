
import datetime
import logging
import os
import requests
import json
import azure.functions as func

# Explicitly configure logging to output to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def call_weather_api_and_get_data():
    api_url = os.getenv("PUBLIC_WEATHER_DATA_API_ENDPOINT")
    api_key = os.getenv("PUBLIC_WEATHER_DATA_API_KEY")

    try:
        if not api_url or not api_key:
            raise ValueError("환경변수 PUBLIC_WEATHER_DATA_API_ENDPOINT 또는 PUBLIC_WEATHER_DATA_API_KEY가 설정되지 않았습니다.")

        now_utc = datetime.datetime.utcnow()
        one_hour_ago_utc = now_utc - datetime.timedelta(hours=1)

        params = {
            "tm1": one_hour_ago_utc.strftime("%Y%m%d%H%M"),
            "tm2": now_utc.strftime("%Y%m%d%H%M"),
            "stn": "0",
            "help": "1",
            "authKey": api_key,
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        logging.info("날씨 API 호출 성공")
        return response.text

    except Exception as e:
        logging.error("날씨 API 호출 실패: %s", e)
        return None



def get_seoul_population_data():
    logging.info("행정동 단위 서울 생활인구(내국인)")
    api_url = os.getenv("SEOUL_POPULATION_API_ENDPOINT")
    api_key = os.getenv("SEOUL_POPULATION_API_KEY")

    try:
        if not api_url or not api_key:
            raise ValueError("환경변수 SEOUL_POPULATION_API_ENDPOINT 또는 SEOUL_POPULATION_API_KEY가 설정되지 않았습니다.")

        # TODO: Add specific parameters for Seoul Population API if needed
        params = {
            "key": api_key,
            "type": "json", 
            "service": "SPOP_LOCAL_RESD_DONG",
            "startIndex": "1", 
            "endIndex": "10"
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }


        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    except Exception as e:
        logging.error("서울 생활인구 API 호출 실패: %s", e)
        return None

def get_sdot_floating_population_data():
    api_url = os.getenv("SDOT_POPULATION_API_ENDPOINT")
    api_key = os.getenv("SDOT_POPULATION_API_KEY")
    try:
        if not api_url or not api_key:
            raise ValueError("환경변수 SDOT_POPULATION_API_ENDPOINT 또는 SDOT_POPULATION_API_KEY가 설정되지 않았습니다.")
        service = "IotVdata018"
        startIndex = "1"
        endIndex = "10"
        
        full_api_url = f"{api_url}/{api_key}/json/{service}/{startIndex}/{endIndex}/"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(full_api_url, headers=headers)
        response.raise_for_status()
        logging.info("S-DoT 유동인구 API 호출 성공")
        return response.json()
    
    except Exception as e:
        logging.error("S-DoT 유동인구 API 호출 실패: %s", e)
        return None
    return "S-DoT Floating Population Data Placeholder" 

def get_administrative_district_codes():
    logging.info("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 API 호출")
    api_url = os.getenv("GGINSTCODE_API_ENDPOINT")
    api_key = os.getenv("GGINSTCODE_API_KEY")

    try:
        if not api_url or not api_key:
            raise ValueError("환경변수 GGINSTCODE_API_ENDPOINT 또는 GGINSTCODE_API_KEY가 설정되지 않았습니다.")

        params = {
            "KEY": api_key,
            "Type": "json",
            "pIndex": "1",
            "pSize": "10"
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        logging.info("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 API 호출 성공")
        return response.json()

    except Exception as e:
        logging.error("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 API 호출 실패: %s", e)
        return None

def main(timer_trigger1: func.TimerRequest, outputEventHub: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().isoformat()
    logging.info('Timer trigger executed at %s', utc_timestamp)

    weather_data = call_weather_api_and_get_data()
    if weather_data:
        logging.info("날씨 데이터 Event Hub로 전송: %s", weather_data)
        outputEventHub.set(weather_data)
        logging.info("날씨 데이터 Event Hub로 전송 완료")
    else:
        logging.warning("날씨 데이터를 가져오지 못하여 Event Hub로 전송하지 않습니다.")


    seoul_population_data = get_seoul_population_data()
    if seoul_population_data:
        seoul_population_json = json.dumps(seoul_population_data)
        logging.info("서울 생활인구 데이터 Event Hub로 전송: %s", seoul_population_json)
        outputEventHub.set(seoul_population_json)
        logging.info("서울 생활인구 데이터 Event Hub로 전송 완료")
    else:
        logging.warning("서울 생활인구 데이터를 가져오지 못하여 Event Hub로 전송하지 않습니다.")

    sdot_floating_population_data = get_sdot_floating_population_data()
    if sdot_floating_population_data:
        sdot_floating_population_json = json.dumps(sdot_floating_population_data)
        logging.info("스마트서울 도시데이터 센서(S-DoT) 유동인구 데이터 Event Hub로 전송: %s", sdot_floating_population_json)
        outputEventHub.set(sdot_floating_population_json)
        logging.info("스마트서울 도시데이터 센서(S-DoT) 유동인구 데이터 Event Hub로 전송 완료")
    else:
        logging.warning("스마트서울 도시데이터 센서(S-DoT) 유동인구 데이터를 가져오지 못하여 Event Hub로 전송하지 않습니다.")

    
    # MS ADLS Parquet 파일 가져오려다 취소.
    # conversation Table 만들어서 DB 쿼리하게 FK 참조하는 구조로 수정
    # administrative_district_codes_data = get_administrative_district_codes()
    # if administrative_district_codes_data:
    #    administrative_district_codes_json = json.dumps(administrative_district_codes_data)
    #    logging.info("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 데이터 Event Hub로 전송: %s", administrative_district_codes_json)
    #    outputEventHub.set(administrative_district_codes_json)
    #    logging.info("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 데이터 Event Hub로 전송 완료")
    #else:
    #    logging.warning("경기도 행정기관 읍면동 단위의 행정동 및 법정동 코드표 데이터를 가져오지 못하여 Event Hub로 전송하지 않습니다.")
    



        

