#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

API_KEY = os.getenv("MOLIT_STATS_KEY")
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"


def fetch_entries(form_id: int, start_dt: str, end_dt: str) -> list[dict]:
    """
    form_id에 해당하는 데이터를 pagination 처리해 전부 가져옵니다.
    """
    items = []
    page = 1
    while True:
        params = {
            "key": API_KEY,
            "form_id": form_id,
            "style_num": 1,
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
        # 단일 dict일 수도, list일 수도 있음
        if isinstance(batch, dict):
            items.append(batch)
        else:
            items.extend(batch)
        if len(batch) < params["numOfRows"]:
            break
        page += 1
    return items


def fetch_monthly_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    data = fetch_entries(form_id=5329, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    # "호" 컬럼이 있으면 "미분양호수"로 rename
    if "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    data = fetch_entries(form_id=5328, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    if "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    return df


def parse_region_hierarchy(region_name: str) -> list[str]:
    """
    region_name을 공백으로 split.
    - len==1: ['경기도']
    - len==2: ['경기도 수원시']
    - len>=3: ['경기도', '경기도 수원시']
    """
    parts = region_name.split()
    if len(parts) == 1:
        return [parts[0]]
    elif len(parts) == 2:
        return [" ".join(parts)]
    else:
        # 3단계 이상: 시도만, 시도+시군구
        return [parts[0], " ".join(parts[:2])]


def filter_by_region(df: pd.DataFrame, province: str, city: str | None) -> pd.DataFrame:
    """
    DataFrame에 있는 '시도명' == province, (있으면) '시군구명' == city 로 필터링
    """
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

    # 1) 지역 계층 리스트 만들기
    regions = parse_region_hierarchy(args.region_name)

    # 2) raw 데이터 한 번만 불러오기
    df_monthly_raw = fetch_monthly_notsold(args.start, args.end)
    df_completed_raw = fetch_completed_notsold(args.start, args.end)

    # 3) 각 지역별로 필터 → merge → dict에 저장
    merged_dict: dict[str, pd.DataFrame] = {}
    for reg in regions:
        parts = reg.split()
        province = parts[0]
        city = parts[1] if len(parts) > 1 else None

        df_m = filter_by_region(df_monthly_raw, province, city)
        df_c = filter_by_region(df_completed_raw, province, city)

        # 날짜 컬럼 자동 탐지 (예: 'stdDay' 또는 'date')
        if "stdDay" in df_m.columns:
            date_col = "stdDay"
        elif "date" in df_m.columns:
            date_col = "date"
        else:
            # fallback: 첫 번째 컬럼
            date_col = df_m.columns[0]

        merged = (
            df_m[[date_col, "미분양호수"]]
            .merge(df_c[[date_col, "완료후미분양호수"]], on=date_col, how="left")
        )
        merged_dict[reg] = merged

    # 4) ExcelWriter로 각 시트에 기록
    with pd.ExcelWriter(args.output) as writer:
        for reg_name, df_out in merged_dict.items():
            # 시트명은 최대 31자, 공백→언더바 변환
            sheet = reg_name.replace(" ", "_")[:31]
            df_out.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
