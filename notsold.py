#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

API_KEY = os.getenv("MOLIT_STATS_KEY")
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"


def fetch_entries(form_id: int, style_num: int, start_dt: str, end_dt: str) -> list[dict]:
    """
    form_id, style_num에 해당하는 데이터를 pagination 처리해 전부 가져옵니다.
    """
    items = []
    page = 1
    while True:
        params = {
            "key": API_KEY,
            "form_id": form_id,
            "style_num": style_num,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "pageNo": page,
            "numOfRows": 1000,
            "resultType": "json",
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        body = r.json().get("response", {}).get("body", {})
        batch = body.get("items", {}).get("item")
        if not batch:
            break
        if isinstance(batch, dict):
            items.append(batch)
        else:
            items.extend(batch)
        if len(batch) < params["numOfRows"]:
            break
        page += 1
    return items


def fetch_monthly_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    # form_id=2082, style_num=128 으로 변경
    data = fetch_entries(form_id=2082, style_num=128, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    if "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    # form_id=5328, style_num=1 (기존)
    data = fetch_entries(form_id=5328, style_num=1, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    if "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    return df


def parse_region_hierarchy(region_name: str) -> list[str]:
    parts = region_name.split()
    if len(parts) == 1:
        return [parts[0]]
    elif len(parts) == 2:
        return [" ".join(parts)]
    else:
        return [parts[0], " ".join(parts[:2])]


def filter_by_region(df: pd.DataFrame, province: str, city: str | None) -> pd.DataFrame:
    mask = df["시도명"] == province
    if city:
        mask &= df["시군구명"] == city
    return df.loc[mask].copy()


def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument(
        "--region-name",
        required=True,
        help="예: 서울, 서울 종로구, 경기도 수원시 영통구",
    )
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end", required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    regions = parse_region_hierarchy(args.region_name)

    df_monthly_raw = fetch_monthly_notsold(args.start, args.end)
    df_completed_raw = fetch_completed_notsold(args.start, args.end)

    merged_dict: dict[str, pd.DataFrame] = {}
    for reg in regions:
        parts = reg.split()
        province = parts[0]
        city = parts[1] if len(parts) > 1 else None

        df_m = filter_by_region(df_monthly_raw, province, city)
        df_c = filter_by_region(df_completed_raw, province, city)

        if "stdDay" in df_m.columns:
            date_col = "stdDay"
        elif "date" in df_m.columns:
            date_col = "date"
        else:
            date_col = df_m.columns[0]

        merged = (
            df_m[[date_col, "미분양호수"]]
            .merge(df_c[[date_col, "완료후미분양호수"]], on=date_col, how="left")
        )
        merged_dict[reg] = merged

    with pd.ExcelWriter(args.output) as writer:
        for reg_name, df_out in merged_dict.items():
            sheet = reg_name.replace(" ", "_")[:31]
            df_out.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
