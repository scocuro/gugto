#!/usr/bin/env python3
# notsold.py

import argparse
import os
import sys

import pandas as pd
import requests

API_KEY = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    print("❌ 환경 변수 MOLIT_STATS_KEY 가 설정되어 있지 않습니다.", file=sys.stderr)
    sys.exit(1)

BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"

# 입력된 시도명을 API의 '구분' 값으로 매핑
PROVINCE_MAP = {
    "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구", "인천광역시": "인천",
    "광주광역시": "광주", "대전광역시": "대전", "울산광역시": "울산", "세종특별자치시": "세종",
    "경기도": "경기", "강원도": "강원", "충청북도": "충북", "충청남도": "충남",
    "전라북도": "전북", "전라남도": "전남", "경상북도": "경북", "경상남도": "경남",
    "제주특별자치도": "제주", "제주도": "제주",
}

def parse_region(region_name: str):
    parts = region_name.strip().split()
    if len(parts) == 1:
        p_in, c_in, d_in = parts[0], None, None
    elif len(parts) == 2:
        p_in, c_in, d_in = parts[0], parts[1], None
    else:
        p_in, c_in, d_in = parts[0], parts[1], parts[2]

    prov = PROVINCE_MAP.get(p_in)
    if not prov:
        prov = p_in.rstrip("도")
        if prov not in PROVINCE_MAP.values():
            raise ValueError(f"지원하지 않는 시도명입니다: '{p_in}'")
    return prov, c_in, d_in

def fetch_data(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    params = {
        "key": API_KEY,
        "form_id": form_id,
        "style_num": style_num,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "pageNo": 1,
        "numOfRows": 1000,
        "resultType": "json",
    }
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    js = r.json()
    items = js.get("result_data", {}).get("formList")
    if items is None:
        raise KeyError(f"응답 형식이 예상과 다릅니다: {list(js.keys())}")
    return pd.DataFrame(items)

def main():
    parser = argparse.ArgumentParser(description="미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: '경기도 수원시 영통구' 또는 '충청남도'")
    parser.add_argument("--start",      required=True, help="시작 월 YYYYMM")
    parser.add_argument("--end",        required=True, help="종료 월 YYYYMM")
    parser.add_argument("--output",     default="notsold.xlsx",
                        help="출력 엑셀 파일명")
    args = parser.parse_args()

    prov, city, dist = parse_region(args.region_name)

    # 1) 시군구별 월별 미분양 (form_id=2082, style_num=128)
    df_mon = fetch_data(2082, 128, args.start, args.end)
    df_mon = df_mon.rename(columns={"호": "미분양현황"})
    # 시도별로 먼저 필터
    df_mon = df_mon[df_mon["구분"] == prov]

    # 2) 공사완료 후 미분양 (form_id=5328, style_num=1)
    df_cmp = fetch_data(5328, 1, args.start, args.end)
    df_cmp = df_cmp.rename(columns={"호": "공사완료후미분양호수"})
    # 여기서만 부문·규모 == '계' 필터
    df_cmp = df_cmp[(df_cmp["부문"] == "계") & (df_cmp["규모"] == "계")]
    df_cmp = df_cmp[df_cmp["구분"] == prov]

    # 병합해서 시트별로 저장
    sheets = {}
    def make_sheet(name, m_df, c_df):
        m = m_df[["date", "미분양현황"]].copy()
        c = c_df[["date", "공사완료후미분양호수"]].copy()
        out = pd.merge(m, c, on="date", how="left")
        sheets[name] = out

    # (1) 도 전체
    make_sheet(prov, df_mon, df_cmp)
    # (2) 시 단위
    if city:
        make_sheet(city,
                   df_mon[df_mon["시군구"] == city],
                   df_cmp[df_cmp["시군구"] == city])
    # (3) 구 단위
    if dist:
        make_sheet(dist,
                   df_mon[df_mon["시군구"] == dist],
                   df_cmp[df_cmp["시군구"] == dist])

    # Excel 쓰기
    with pd.ExcelWriter(args.output) as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
