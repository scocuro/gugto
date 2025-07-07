#!/usr/bin/env python3
# notsold.py

import os
import argparse
import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────────────────────
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
API_KEY   = os.getenv("MOLIT_STATS_KEY")  # MOLIT_STATS_KEY 환경변수에 키를 설정하세요

def fetch_json_list(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    MOLIT API에서 form_id/style_num으로 페이징 전수 조회 후 DataFrame 반환.
    """
    params = {
        "key":        API_KEY,
        "form_id":    form_id,
        "style_num":  style_num,
        "start_dt":   start_dt,
        "end_dt":     end_dt,
        "resultType": "json",
        "pageNo":     1,
        "numOfRows":  1000,
    }
    items = []
    while True:
        resp = requests.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json().get("formList", [])
        if not data:
            break
        items.extend(data)
        if len(data) < params["numOfRows"]:
            break
        params["pageNo"] += 1
    return pd.DataFrame(items)


def merge_counts(df_mon: pd.DataFrame, df_cmp: pd.DataFrame) -> pd.DataFrame:
    """
    월별(df_mon)과 완료후(df_cmp)를 date 기준 병합하고 컬럼명 변경.
    """
    mon = df_mon.rename(columns={"호": "미분양호수"})
    cmp = df_cmp.rename(columns={"호": "완료후미분양호수"})
    return mon.merge(
        cmp[["date", "완료후미분양호수"]],
        on="date", how="left"
    )


def filter_region(df: pd.DataFrame, province: str, subregion: str = None) -> pd.DataFrame:
    """
    df에서
      - 구분==province
      - 시군구==subregion (None이면 '계' 요약행)
    """
    df0 = df[df["구분"] == province]
    if subregion:
        return df0[df0["시군구"] == subregion]
    else:
        return df0[df0["시군구"] == "계"]


def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: 경기도  OR  경기도 수원시  OR  경기도 수원시 영통구")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx",
                        help="저장할 Excel 파일명")
    args = parser.parse_args()

    # 입력 분해
    parts    = args.region_name.split()
    province = parts[0]
    city     = parts[1] if len(parts) >= 2 else None
    district = parts[2] if len(parts) >= 3 else None

    # 1) 월별미분양(form_id=2082, style_num=128)
    # 2) 공사완료후미분양(form_id=5328, style_num=1)
    df_monthly_all   = fetch_json_list(2082, 128, args.start, args.end)
    df_completed_all = fetch_json_list(5328,   1, args.start, args.end)

    # 결과를 한 파일에 여러 시트로 저장
    with pd.ExcelWriter(args.output) as writer:
        # ── ① 도(省) 단위
        mon_p = filter_region(df_monthly_all,   province)
        cmp_p = filter_region(df_completed_all, province)
        merge_counts(mon_p, cmp_p) \
            .to_excel(writer, sheet_name=province, index=False)

        # ── ② 시(市) 단위
        if city:
            mon_c = filter_region(df_monthly_all,   province, city)
            cmp_c = filter_region(df_completed_all, province, city)
            merge_counts(mon_c, cmp_c) \
                .to_excel(writer,
                          sheet_name=f"{_
