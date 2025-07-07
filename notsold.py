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
    """
    월별 미분양 (form_id=2082, style_num=128)
    """
    data = fetch_entries(2082, 128, start_dt, end_dt)
    df = pd.DataFrame(data)
    # 컬럼 앞뒤 공백 제거
    df.columns = df.columns.str.strip()
    # '미분양현황' → '미분양호수'
    if "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "미분양호수"})
    elif "호" in df.columns:
        df = df.rename(columns={"호": "미분양호수"})
    return df


def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    공사완료후 미분양 (form_id=5328, style_num=1)
    """
    data = fetch_entries(5328, 1, start_dt, end_dt)
    df = pd.DataFrame(data)
    # 컬럼 앞뒤 공백 제거
    df.columns = df.columns.str.strip()
    # '호' → '완료후미분양호수', 혹은 '미분양현황' 컬럼이 있을 경우
    if "호" in df.columns:
        df = df.rename(columns={"호": "완료후미분양호수"})
    elif "미분양현황" in df.columns:
        df = df.rename(columns={"미분양현황": "완료후미분양호수"})
    return df


def parse_region_hierarchy(region_name: str) -> list[str]:
    """
    입력된 region_name을
      - ["서울"] 또는
      - ["경기도", "경기도 수원시"]
    처럼 Province- 또는 Province+City- 레벨 두 단계로 분해합니다.
    """
    parts = region_name.split()
    if len(parts) == 1:
        return [parts[0]]
    return [parts[0], " ".join(parts[:2])]


def filter_by_region(df: pd.DataFrame, province: str, city: str | None = None) -> pd.DataFrame:
    """
    df["구분"] 에 province가, 
    city가 주어지면 df["시군구"] == city, 없으면 df["시군구"] == "계" 로 필터링합니다.
    """
    # 컬럼명도 공백 제거
    df.columns = df.columns.str.strip()
    if "구
