#!/usr/bin/env python3
# notsold.py

import argparse
import pandas as pd
from common import fetch_json_list, split_region_name

# ── API 엔드포인트 ──
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"

def fetch_monthly_notsold(start_dt, end_dt):
    """월별 미분양 (form_id=5329) 전체 기간 한번에 가져오기"""
    params = {
        "form_id":   5329,
        "style_num": 1,
        "start_dt":  start_dt,
        "end_dt":    end_dt,
    }
    data = fetch_json_list(BASE_URL, params, list_key="formList")
    return pd.DataFrame(data)


def fetch_completed_notsold(start_dt, end_dt):
    """공사완료 후 미분양 (form_id=5328) 전체 기간 한번에 가져오기"""
    params = {
        "form_id":   5328,
        "style_num": 1,
        "start_dt":  start_dt,
        "end_dt":    end_dt,
    }
    data = fetch_json_list(BASE_URL, params, list_key="formList")
    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: 경기도 또는 경기도 수원시 또는 경기도 수원시 영통구")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx",
                        help="출력 엑셀 파일명")
    args = parser.parse_args()

    # 1) 지역명 분리 → 3단계(省→市→郡)，없는 건 None
    province, city, district = split_region_name(args.region_name)

    # 2) 한 번씩 API에서 가져온 뒤 DataFrame 생성
    df_monthly   = fetch_monthly_notsold(args.start, args.end)
    df_completed = fetch_completed_notsold(args.start, args.end)

    # 3) 도/시/구(읍면동) 레벨별 필터 정의
    levels = []
    # 3-1) 도 레벨
    levels.append(("{}_월별".format(province),
                   df_monthly[df_monthly["시도"] == province],
                   df_completed[df_completed["시도"] == province]))
    # 3-2) 시군 레벨 (있으면)
    if city:
        name = f"{province} {city}"
        levels.append((f"{name}_월별",
                       df_monthly[(df_monthly["시도"] == province) &
                                  (df_monthly["시군구"] == city)],
                       df_completed[(df_completed["시도"] == province) &
                                    (df_completed["시군구"] == city)]))
    # 3-3) 읍면동 레벨 (있으면)
    if district:
        name = f"{province} {city} {district}"
        levels.append((f"{name}_월별",
                       df_monthly[(df_monthly["시도"] == province)  &
                                  (df_monthly["시군구"] == city)     &
                                  (df_monthly["읍면동"] == district)],
                       df_completed[(df_completed["시도"] == province)  &
                                    (df_completed["시군구"] == city)     &
                                    (df_completed["읍면동"] == district)]))

    # 4) 결과를 하나의 Excel 파일에 레벨별 시트로 저장
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        for sheet_prefix, df_mon, df_comp in levels:
            # 월별
            df_mon.to_excel(writer,
                            sheet_name=sheet_prefix,
                            index=False)
            # 완료후
            df_comp.to_excel(writer,
                              sheet_name=sheet_prefix.replace("_월별", "_완료후"),
                              index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
