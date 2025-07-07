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
    주어진 form_id와 style_num에 대해 모든 페이지를 조회하여 항목들을 리스트로 반환합니다.
    """
    items: list[dict] = []
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
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        body = response.json().get("response", {}).get("body", {})
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
    """
    월별 미분양(form_id=2082, style_num=128)을 조회하여 DataFrame으로 반환합니다.
    """
    data = fetch_entries(2082, 128, start_dt, end_dt)
    df = pd.DataFrame(data)
    # 컬럼명 앞뒤 공백 제거
    df.columns = df.columns.str.strip()
    # 미분양 수량 컬럼명 표준화
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "미분양호수"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    공사완료후 미분양(form_id=5328, style_num=1)을 조회하여 DataFrame으로 반환합니다.
    """
    data = fetch_entries(5328, 1, start_dt, end_dt)
    df = pd.DataFrame(data)
    # 컬럼명 앞뒤 공백 제거
    df.columns = df.columns.str.strip()
    # 완료후 미분양 수량 컬럼명 표준화
    if "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    elif "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "완료후미분양호수"})
    return df


def parse_region_hierarchy(region_name: str) -> list[str]:
    """
    입력된 region_name을 분해하여 [도/특별시, 도/특별시 시/군/구] 형태의 리스트로 반환합니다.
    예: "경기도 수원시 영통구" -> ["경기도", "경기도 수원시"]
    """
    parts = region_name.split()
    if len(parts) == 1:
        return [parts[0]]
    return [parts[0], " ".join(parts[:2])]


def filter_by_region(df: pd.DataFrame, province: str, city: str | None = None) -> pd.DataFrame:
    """
    df의 '구분'(도/특별시) 및 '시군구' 컬럼으로 필터링합니다.
    city가 None일 경우 '시군구' == '계'만 추출합니다.
    """
    df.columns = df.columns.str.strip()
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

    regions = parse_region_hierarchy(args.region_name)

    df_monthly   = fetch_monthly_notsold(args.start, args.end)
    df_completed = fetch_completed_notsold(args.start, args.end)

    with pd.ExcelWriter(args.output) as writer:
        for reg in regions:
            parts = reg.split()
            province = parts[0]
            city     = parts[1] if len(parts) > 1 else None

            m = filter_by_region(df_monthly, province, city)
            c = filter_by_region(df_completed, province, city)

            # 날짜 컬럼 자동 선택
            date_col = next((col for col in ("date", "stdDay") if col in m.columns), m.columns[0])

            out = (
                m[[date_col, "미분양호수"]]
                .merge(c[[date_col, "완료후미분양호수"]], on=date_col, how="left")
            )
            sheet_name = reg.replace(" ", "_")[:31]
            out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
