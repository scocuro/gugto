#!/usr/bin/env python3
# modules/notsold.py

import argparse
import os
import pandas as pd
from common import fetch_json_list

# MOLIT 통계서비스 API 키를 환경변수에서 읽어옵니다
API_KEY = os.getenv("MOLIT_STATS_KEY")
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"


def parse_region_hierarchy(region_name: str):
    """
    입력된 지명을 도/시, 시/군/구 수준으로 분해합니다.
    예: "경기도 수원시 영통구" → ("경기도", "수원시", "영통구")
    """
    parts = region_name.split()
    province = parts[0] if len(parts) >= 1 else None
    city     = parts[1] if len(parts) >= 2 else None
    district = parts[2] if len(parts) >= 3 else None
    return province, city, district


def fetch_monthly_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    월별 미분양 현황(form_id=2082, style_num=128)을 가져와 DataFrame으로 반환합니다.
    """
    params = {
        "key": API_KEY,
        "form_id": 2082,
        "style_num": 128,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    data = fetch_json_list(BASE_URL, params)
    df = pd.DataFrame(data)
    # 컬럼명이 문자열이 아닐 수 있으므로, 안전하게 strip 처리
    df.columns = [str(col).strip() for col in df.columns]
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    공사완료 후 미분양(form_id=5328, style_num=1)을 가져와 DataFrame으로 반환합니다.
    """
    params = {
        "key": API_KEY,
        "form_id": 5328,
        "style_num": 1,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    data = fetch_json_list(BASE_URL, params)
    df = pd.DataFrame(data)
    df.columns = [str(col).strip() for col in df.columns]
    return df


def filter_by_region(df: pd.DataFrame, province: str, city: str = None, district: str = None) -> pd.DataFrame:
    """
    df에서 도/시, 시/군/구 수준으로 필터링합니다.
    - district가 있으면 시군구별 '구분'에서 district를 필터
    - city만 있으면 시군구별 '구분'에서 city를 필터
    - 둘 다 없으면 시도별 '구분'에서 province를 필터
    """
    if district:
        mask = (df['구분'] == '시군구별') & (df['시군구'] == district)
    elif city:
        mask = (df['구분'] == '시군구별') & (df['시군구'] == city)
    else:
        mask = (df['구분'] == '시도별') & (df['시군구'] == province)
    return df[mask]


def main():
    parser = argparse.ArgumentParser(description="미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: 경기도 수원시 영통구 또는 경상남도")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    province, city, district = parse_region_hierarchy(args.region_name)

    # 원시 데이터 조회
    df_monthly_raw   = fetch_monthly_notsold(args.start, args.end)
    df_completed_raw = fetch_completed_notsold(args.start, args.end)

    # 각 단계별로 필터링하여 시트로 저장
    sheets = {
        'monthly_province':  filter_by_region(df_monthly_raw,   province),
        'completed_province': filter_by_region(df_completed_raw, province),
    }
    if city:
        sheets['monthly_city']     = filter_by_region(df_monthly_raw,   province, city)
        sheets['completed_city']   = filter_by_region(df_completed_raw, province, city)
    if district:
        sheets['monthly_district']   = filter_by_region(df_monthly_raw,   province, city, district)
        sheets['completed_district'] = filter_by_region(df_completed_raw, province, city, district)

    # 결과를 엑셀 파일로 출력
    with pd.ExcelWriter(args.output) as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
