#!/usr/bin/env python3
# modules/notsold.py

import argparse
import os
import pandas as pd
import requests

from common import fetch_json_list

API_KEY = os.getenv("MOLIT_STATS_KEY")  # .env 에서 a5fc3ec18fd14b10bb497ac72138ebc1 로 설정

def fetch_completed_notsold(province, district, start_dt, end_dt):
    """공사완료후 미분양(form_id=5328)"""
    url = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key": API_KEY,
        "form_id": 5328,
        "style_num": 1,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    data = fetch_json_list(url, params, list_key="formList")
    df = pd.DataFrame(data)
    return df[(df["구분"] == province) & (df["시군구"] == district)]

def fetch_monthly_notsold(province, district, start_dt, end_dt):
    """월별 미분양(form_id=5329 등 실제 ID 확인 필요)"""
    url = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key": API_KEY,
        "form_id": 5329,  # 예시
        "style_num": 1,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    data = fetch_json_list(url, params, list_key="formList")
    df = pd.DataFrame(data)
    return df[(df["구분"] == province) & (df["시군구"] == district)]

def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: 서울 종로구")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    parts = args.region_name.strip().split()
    if len(parts) < 2:
        parser.error("`--region-name` 은 반드시 시도와 시군구를 띄어쓰기해 주세요. (예: '인천 서구')")
    province = parts[0]                          # 첫 토큰: 시도 (ex. "서울", "부산", "경기도")
    district = "".join(parts[1:]) if len(parts) > 2 else parts[1]

    df1 = fetch_monthly_notsold(province, district, args.start, args.end)
    df2 = fetch_completed_notsold(province, district, args.start, args.end)

    # 날짜별로 merge
    out = (df1.rename(columns={"호":"미분양호수"})
             .merge(df2.rename(columns={"호":"완료후미분양호수"}),
                    on="date", how="left"))
    out.to_excel(args.output, index=False)
    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
