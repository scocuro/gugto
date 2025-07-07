#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

# 로컬 헬퍼: MOLIT API의 페이징된 데이터를 모두 가져옵니다.
def fetch_form_list(url, params, list_key="formList"):
    results = []
    page = 1
    while True:
        page_params = params.copy()
        page_params.update({
            "pageNo": page,
            "numOfRows": 1000,
            "resultType": "json"
        })
        response = requests.get(url, params=page_params)
        response.raise_for_status()
        data = response.json()
        items = data.get("result_data", {}).get(list_key, [])
        if not items:
            break
        results.extend(items)
        if len(items) < page_params["numOfRows"]:
            break
        page += 1
    return results

# 입력된 도 이름을 API의 '구분' 컬럼 키로 매핑합니다.
PROVINCE_KEY_MAP = {
    "서울특별시": "서울", "서울": "서울",
    "부산광역시": "부산", "부산": "부산",
    "대구광역시": "대구", "대구": "대구",
    "인천광역시": "인천", "인천": "인천",
    "광주광역시": "광주", "광주": "광주",
    "대전광역시": "대전", "대전": "대전",
    "울산광역시": "울산", "울산": "울산",
    "세종특별자치시": "세종", "세종": "세종",
    "제주특별자치도": "제주", "제주": "제주",
    "경기도": "경기", "강원도": "강원",
    "충청북도": "충북", "충청남도": "충남",
    "전라북도": "전북", "전라남도": "전남",
    "경상북도": "경북", "경상남도": "경남"
}

def parse_region_name(region_name):
    parts = region_name.strip().split()
    prov_input = parts[0]
    province_key = PROVINCE_KEY_MAP.get(prov_input, prov_input)
    city_key = parts[1] if len(parts) >= 2 else None
    district_key = parts[2] if len(parts) >= 3 else None
    return province_key, city_key, district_key


def fetch_monthly_notsold(start_dt, end_dt, api_key):
    """월별 미분양 (form_id=2082, style_num=128)"""
    BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key": api_key,
        "form_id": 2082,
        "style_num": 128,
        "start_dt": start_dt,
        "end_dt": end_dt
    }
    data = fetch_form_list(BASE_URL, params)
    df = pd.DataFrame(data)
    # 컬럼 이름 통일
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "미분양호수"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    else:
        raise KeyError(f"미분양호수 컬럼을 찾을 수 없습니다: {df.columns.tolist()}")
    df.columns = df.columns.str.strip()
    return df


def fetch_completed_notsold(start_dt, end_dt, api_key):
    """공사완료후 미분양 (form_id=5328, style_num=1)"""
    BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key": api_key,
        "form_id": 5328,
        "style_num": 1,
        "start_dt": start_dt,
        "end_dt": end_dt
    }
    data = fetch_form_list(BASE_URL, params)
    df = pd.DataFrame(data)
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "완료후미분양호수"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    else:
        raise KeyError(f"완료후미분양호수 컬럼을 찾을 수 없습니다: {df.columns.tolist()}")
    df.columns = df.columns.str.strip()
    return df


def filter_by_region(df, province_key, city_key=None, district_key=None):
    # 도별 합계(시군구 == '계')
    df_prov = df[(df.get("구분") == province_key) & (df.get("시군구") == "계")]
    df_city = df[(df.get("구분") == province_key) & (df.get("시군구") == city_key)] if city_key else None
    df_district = df[(df.get("구분") == province_key) & (df.get("시군구") == district_key)] if district_key else None
    return df_prov, df_city, df_district


def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument("--region-name", required=True, help="예: 경기도 수원시 영통구")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end", required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    api_key = os.getenv("MOLIT_STATS_KEY")
    if not api_key:
        raise EnvironmentError("환경변수 MOLIT_STATS_KEY가 설정되어 있지 않습니다.")

    province_key, city_key, district_key = parse_region_name(args.region_name)

    # 데이터 수집
    df_monthly = fetch_monthly_notsold(args.start, args.end, api_key)
    df_completed = fetch_completed_notsold(args.start, args.end, api_key)

    # 지역별 필터링
    monthly_prov, monthly_city, monthly_district = filter_by_region(df_monthly, province_key, city_key, district_key)
    comp_prov, comp_city, comp_district = filter_by_region(df_completed, province_key, city_key, district_key)

    # Excel로 내보내기 (다중 시트)
    with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
        # 도
        if not monthly_prov.empty:
            monthly_prov[['date','미분양호수']].to_excel(writer, sheet_name=f"{province_key}_월별", index=False)
            comp_prov[['date','완료후미분양호수']].to_excel(writer, sheet_name=f"{province_key}_완료후", index=False)
        # 시
        if city_key and monthly_city is not None and not monthly_city.empty:
            monthly_city[['date','미분양호수']].to_excel(writer, sheet_name=f"{city_key}_월별", index=False)
            comp_city[['date','완료후미분양호수']].to_excel(writer, sheet_name=f"{city_key}_완료후", index=False)
        # 구
        if district_key and monthly_district is not None and not monthly_district.empty:
            monthly_district[['date','미분양호수']].to_excel(writer, sheet_name=f"{district_key}_월별", index=False)
            comp_district[['date','완료후미분양호수']].to_excel(writer, sheet_name=f"{district_key}_완료후", index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
