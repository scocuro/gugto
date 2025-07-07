#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

MOLIT_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
API_KEY = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    raise EnvironmentError(
        "환경변수 MOLIT_STATS_KEY 에 API 키를 설정해주세요"
    )

def fetch_data(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    MOLIT API 호출 후 DataFrame으로 반환.
    - formList 중 ['date','구분','시군구', count] 컬럼만 남김
    - count 컬럼명은 unitName 에 따라 '미분양현황' 또는 '호' 일 수 있으니 'count'로 통일
    """
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

    # 컬럼명 통일
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "count"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "count"})
    else:
        raise KeyError(f"필수 컬럼 '미분양현황' 또는 '호' 가 응답에 없습니다: {df.columns.tolist()}")

    # 필수 컬럼 검증
    for col in ("date", "구분", "시군구"):
        if col not in df.columns:
            raise KeyError(f"필수 컬럼 '{col}' 가 응답에 없습니다: {df.columns.tolist()}")

    return df[["date", "구분", "시군구", "count"]]

def filter_by_region(df: pd.DataFrame, province: str, city: str = None):
    """
    df에서
      - province(구분)==province, 시군구=="계" → 도별
      - province(구분)==province, 시군구==city → 시군구별 (city가 있을 때)
    """
    df_prov = df[(df["구분"] == province) & (df["시군구"] == "계")].copy()
    if city:
        df_city = df[(df["구분"] == province) & (df["시군구"] == city)].copy()
        return df_prov, df_city
    return df_prov, None

def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument(
        "--region-name", required=True,
        help="예: 경기도, 경상남도 진주시, 경기도 수원시 영통구"
    )
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument(
        "--output", default="notsold_report.xlsx",
        help="출력할 엑셀 파일명 (기본: notsold_report.xlsx)"
    )
    args = parser.parse_args()

    # 입력값 파싱: "도 [시군구 [구]]" → province, city
    parts = args.region_name.split()
    province = parts[0]
    city    = parts[1] if len(parts) >= 2 else None

    # 1) 월별 미분양
    df_monthly   = fetch_data(form_id=2082, style_num=128,
                              start_dt=args.start, end_dt=args.end)
    # 2) 공사완료 후 미분양
    df_completed = fetch_data(form_id=5328, style_num=1,
                              start_dt=args.start, end_dt=args.end)

    # 도/시군구별로 필터링
    prov_monthly, city_monthly     = filter_by_region(df_monthly,   province, city)
    prov_completed, city_completed = filter_by_region(df_completed, province, city)

    # 병합 후 컬럼 정리
    prov_merged = pd.DataFrame({
        "date":              prov_monthly["date"],
        "monthly_count":     prov_monthly["count"],
        "completed_count":   prov_completed["count"],
    })

    # Excel로 저장
    with pd.ExcelWriter(args.output) as writer:
        prov_merged.to_excel(writer, sheet_name="province", index=False)

        if city:
            city_merged = pd.DataFrame({
                "date":            city_monthly["date"],
                "monthly_count":   city_monthly["count"],
                "completed_count": city_completed["count"],
            })
            city_merged.to_excel(writer, sheet_name="city", index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
