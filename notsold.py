#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
from common import get_region_code, fetch_json_list

API_KEY = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    raise SystemExit("ERROR: MOLIT_STATS_KEY 환경변수를 설정하세요.")

def fetch_completed_notsold(start_dt, end_dt):
    """공사완료 후 미분양(form_id=5328) 전체 데이터를 가져와서 반환"""
    url = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key":       API_KEY,
        "form_id":   5328,
        "style_num": 1,
        "start_dt":  start_dt,
        "end_dt":    end_dt,
    }
    data = fetch_json_list(url, params)  # <- region_code 인자 없음
    return pd.DataFrame(data)

def fetch_monthly_notsold(start_dt, end_dt):
    """월별 미분양(form_id=5329) 전체 데이터를 가져와서 반환"""
    url = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
    params = {
        "key":       API_KEY,
        "form_id":   5329,
        "style_num": 1,
        "start_dt":  start_dt,
        "end_dt":    end_dt,
    }
    data = fetch_json_list(url, params)  # <- region_code 인자 없음
    return pd.DataFrame(data)

def parse_args():
    p = argparse.ArgumentParser("미분양 현황 수집기")
    p.add_argument("--region-name", required=True,
                   help="예: 경기도, 경상남도 진주시, 경기도 수원시 영통구")
    p.add_argument("--start", required=True, help="YYYYMM")
    p.add_argument("--end",   required=True, help="YYYYMM")
    p.add_argument("--output", default="notsold.xlsx",
                   help="출력 엑셀 파일명")
    return p.parse_args()

def filter_by_region(df, region_code, level_key="시군구"):
    """DataFrame에서 시군구 컬럼이 region_code와 일치하는 행만 남김"""
    return df[df[level_key] == region_code]

def main():
    args = parse_args()
    parts = args.region_name.split()

    # 1) 대상 지역 레벨 결정
    if len(parts) == 1:
        levels = [parts[0]]                             # ex: ["경기도"]
    elif len(parts) == 2:
        levels = [" ".join(parts)]                     # ex: ["경상남도 진주시"]
    elif len(parts) == 3:
        levels = [
            parts[0],                                  # ex: "경기도"
            " ".join(parts[:2])                        # ex: "경기도 수원시"
        ]
    else:
        raise SystemExit("ERROR: ‘시도’, ‘시도 시군구’, ‘시도 시군구 읍면동’ 형식만 지원합니다.")

    # 2) 전체 데이터 한 번만 가져오기
    df_monthly_all   = fetch_monthly_notsold(args.start, args.end)
    df_completed_all = fetch_completed_notsold(args.start, args.end)

    # 3) 각 레벨별로 코드 조회 & 필터링 → 병합 → 저장 준비
    sheets = {}
    for region in levels:
        try:
            code = get_region_code(region)
        except Exception as e:
            raise SystemExit(f"ERROR: '{region}' 코드 조회 실패: {e}")

        df_m = filter_by_region(df_monthly_all, code, level_key="시군구")
        df_c = filter_by_region(df_completed_all, code, level_key="시군구")

        df_m = df_m.rename(columns={"호": "미분양호수"})
        df_c = df_c.rename(columns={"호": "완료후미분양호수"})
        merged = pd.merge(df_m, df_c, on="date", how="left")

        sheet_name = region.replace(" ", "_")[:31]
        sheets[sheet_name] = merged

    # 4) Excel 파일 쓰기
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        for sheet, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ '{args.output}' 생성 완료:")
    for region in levels:
        print(f"  - {region}")

if __name__ == "__main__":
    main()
