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
    # 월별 미분양: form_id=2082, style_num=128
    data = fetch_entries(form_id=2082, style_num=128, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    if "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    # 공사완료후 미분양: form_id=5328, style_num=1
    data = fetch_entries(form_id=5328, style_num=1, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    if "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    return df


def parse_region_hierarchy(region_name: str) -> list[str]:
    """
    입력된 region_name을
      - ["서울"] 또는
      - ["경기도","경기도 수원시"]
    처럼 Province- or Province+City- 레벨만 뽑아냅니다.
    """
    parts = region_name.split()
    if len(parts) == 1:
        return [parts[0]]
    # 둘 이상 쓰셨다면 province 와 province+city 두 단계만.
    return [parts[0], " ".join(parts[:2])]


def filter_by_region(df: pd.DataFrame, province: str, city: str | None = None) -> pd.DataFrame:
    """
    df["구분"] 에 province가, 
    그리고 city가 주어지면 df["시군구"] == city,
    city가 없으면 aggregate row인 df["시군구"] == "계" 만 남깁니다.
    """
    if "구분" not in df.columns or "시군구" not in df.columns:
        raise KeyError("필수 컬럼 '구분' 혹은 '시군구' 가 없습니다.")
    mask = df["구분"] == province
    if city:
        mask &= df["시군구"] == city
    else:
        mask &= df["시군구"] == "계"
    return df.loc[mask].copy()


def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument(
        "--region-name",
        required=True,
        help="예: 서울, 서울 종로구, 경기도 수원시 영통구"
    )
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    # Province- 레벨, Province+City- 레벨 두 단계
    regions = parse_region_hierarchy(args.region_name)

    df_monthly = fetch_monthly_notsold(args.start, args.end)
    df_completed = fetch_completed_notsold(args.start, args.end)

    # 각 레벨마다 시트 생성
    with pd.ExcelWriter(args.output) as writer:
        for reg in regions:
            parts = reg.split()
            province = parts[0]
            city = parts[1] if len(parts) > 1 else None

            m = filter_by_region(df_monthly, province, city)
            c = filter_by_region(df_completed, province, city)

            # 날짜 컬럼 찾기
            date_col = None
            for col in ("date", "stdDay"):
                if col in m.columns:
                    date_col = col
                    break
            if not date_col:
                date_col = m.columns[0]

            out = (
                m[[date_col, "미분양호수"]]
                .merge(c[[date_col, "완료후미분양호수"]], on=date_col, how="left")
            )

            sheet_name = reg.replace(" ", "_")[:31]
            out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
