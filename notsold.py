#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

MOLIT_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
API_KEY = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    raise EnvironmentError("환경변수 MOLIT_STATS_KEY 에 API 키를 설정해주세요")

def normalize_province(name: str) -> str:
    """
    '경기도'->'경기', '충청남도'->'충남', '서울특별시'->'서울' 등으로 바꿔줍니다.
    """
    suffixes = ["특별자치도","특별자치시","광역시","특별시","도"]
    for s in suffixes:
        if name.endswith(s):
            return name[:-len(s)]
    return name

def fetch_data(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    params = {
        "key": API_KEY,
        "form_id": form_id,
        "style_num": style_num,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "pageNo": 1,
        "numOfRows": 1000,
        "resultType": "json",
    }
    resp = requests.get(MOLIT_URL, params=params)
    resp.raise_for_status()
    data = resp.json()["result_data"]["formList"]
    df = pd.DataFrame(data)

    # '미분양현황' or '호' 컬럼을 'count'로 통일
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "count"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "count"})
    else:
        raise KeyError(f"필수 컬럼 '미분양현황' 또는 '호' 가 없습니다: {df.columns.tolist()}")

    # 필수 컬럼 존재 확인
    for col in ("date", "구분", "시군구"):
        if col not in df.columns:
            raise KeyError(f"필수 컬럼 '{col}' 가 없습니다: {df.columns.tolist()}")

    return df[["date", "구분", "시군구", "count"]]

def filter_by_region(df: pd.DataFrame, province: str, city: str = None):
    """
    province(예: '경기')와 시군구 == '계' 로 도별,
    city(예: '수원시')가 있으면 시군구별(row)도 따로 반환
    """
    df_prov = df[(df["구분"] == province) & (df["시군구"] == "계")].copy()
    df_city = None
    if city:
        df_city = df[(df["구분"] == province) & (df["시군구"] == city)].copy()
    return df_prov, df_city

def main():
    p = argparse.ArgumentParser("미분양 현황 수집기")
    p.add_argument("--region-name", required=True,
                   help="예: '경기도', '충청남도 천안시', '서울특별시 송파구'")
    p.add_argument("--start", required=True, help="YYYYMM")
    p.add_argument("--end",   required=True, help="YYYYMM")
    p.add_argument("--output", default="notsold_report.xlsx",
                   help="엑셀 파일명 (기본: notsold_report.xlsx)")
    args = p.parse_args()

    # region-name 파싱
    parts   = args.region_name.split()
    raw_prov = parts[0]
    prov_key = normalize_province(raw_prov)
    city_key = parts[1] if len(parts) >= 2 else None

    # 디버그: 실제 API가 어떤 '구분'과 '시군구' 값을 주는지 보고 싶으면 uncomment
    # print(">>> prov_key:", prov_key, " city_key:", city_key)

    # 데이터 가져오기
    df_monthly   = fetch_data(2082, 128, args.start, args.end)  # 월별 미분양(form_id=2082, style_num=128)
    df_completed = fetch_data(5328,   1, args.start, args.end)  # 완료후 미분양(form_id=5328, style_num=1)

    # 필터링
    prov_mon, city_mon     = filter_by_region(df_monthly,   prov_key, city_key)
    prov_comp, city_comp   = filter_by_region(df_completed, prov_key, city_key)

    # 병합
    prov_merged = pd.DataFrame({
        "date":            prov_mon["date"],
        "monthly_count":   prov_mon["count"],
        "completed_count": prov_comp["count"],
    })

    # Excel 저장
    with pd.ExcelWriter(args.output) as writer:
        prov_merged.to_excel(writer, sheet_name="province", index=False)

        if city_key:
            city_merged = pd.DataFrame({
                "date":            city_mon["date"],
                "monthly_count":   city_mon["count"],
                "completed_count": city_comp["count"],
            })
            city_merged.to_excel(writer, sheet_name="city", index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
