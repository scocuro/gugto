#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
from common import fetch_json_list

# MOLIT API 키
API_KEY = os.getenv("MOLIT_STATS_KEY")
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"

# 전체 시도명 ↔ API 응답의 '구분' 값 매핑
PROVINCE_MAPPING = {
    '서울특별시': '서울', '서울시': '서울', '서울': '서울',
    '부산광역시': '부산', '부산시': '부산', '부산': '부산',
    '대구광역시': '대구', '대구시': '대구', '대구': '대구',
    '인천광역시': '인천', '인천시': '인천', '인천': '인천',
    '광주광역시': '광주', '광주시': '광주', '광주': '광주',
    '대전광역시': '대전', '대전시': '대전', '대전': '대전',
    '울산광역시': '울산', '울산시': '울산', '울산': '울산',
    '세종특별자치시': '세종', '세종시': '세종', '세종': '세종',
    '경기도': '경기', '경기': '경기',
    '강원도': '강원', '강원': '강원',
    '충청북도': '충북', '충북': '충북',
    '충청남도': '충남', '충남': '충남',
    '전라북도': '전북', '전북': '전북',
    '전라남도': '전남', '전남': '전남',
    '경상북도': '경북', '경북': '경북',
    '경상남도': '경남', '경남': '경남',
    '제주특별자치도': '제주', '제주도': '제주', '제주': '제주'
}

def parse_region(region_name: str):
    """
    region-name을 '경기도', '경기도 수원시', '경기도 수원시 영통구' 등
    최대 3단계까지 받아서,
    시도(code), 시군구(name)로 분리합니다.
    세 번째 단계(영통구)는 현재 city 필터링에는 사용하지 않습니다.
    """
    parts = region_name.strip().split()
    if not parts:
        raise ValueError("`--region-name`을 올바르게 입력해주세요.")
    prov_full = parts[0]
    prov = PROVINCE_MAPPING.get(prov_full)
    if prov is None:
        raise ValueError(f"알 수 없는 시도명: {prov_full}")
    city = parts[1] if len(parts) >= 2 else None
    return prov, city

def fetch_monthly_notsold(prov: str, start_dt: str, end_dt: str) -> pd.DataFrame:
    """월별 미분양 현황 (form_id=2082, style_num=128)"""
    params = {
        "key": API_KEY,
        "form_id": 2082,
        "style_num": 128,
        "start_dt": start_dt,
        "end_dt": end_dt
    }
    data = fetch_json_list(BASE_URL, params, prov, API_KEY)
    df = pd.DataFrame(data)
    df.columns = df.columns.str.strip()
    # 필수 컬럼 체크
    for col in ("date", "호", "시군구", "구분"):
        if col not in df.columns:
            raise KeyError(f"월별 미분양: 필수 컬럼 '{col}' 누락 (응답 컬럼: {df.columns.tolist()})")
    # 필요한 컬럼만 남기고 이름 변경
    df = df[["date", "구분", "시군구", "호"]].rename(columns={"호": "미분양호수"})
    return df

def fetch_completed_notsold(prov: str, start_dt: str, end_dt: str) -> pd.DataFrame:
    """공사완료 후 미분양 (form_id=5328, style_num=1)"""
    params = {
        "key": API_KEY,
        "form_id": 5328,
        "style_num": 1,
        "start_dt": start_dt,
        "end_dt": end_dt
    }
    data = fetch_json_list(BASE_URL, params, prov, API_KEY)
    df = pd.DataFrame(data)
    df.columns = df.columns.str.strip()
    for col in ("date", "호", "시군구", "구분"):
        if col not in df.columns:
            raise KeyError(f"공사완료후 미분양: 필수 컬럼 '{col}' 누락 (응답 컬럼: {df.columns.tolist()})")
    df = df[["date", "구분", "시군구", "호"]].rename(columns={"호": "완료후미분양호수"})
    return df

def filter_region(df: pd.DataFrame, prov: str, city: str):
    """
    prov(시도) 수준 total('계' 또는 '합계')와,
    city(시·군·구) 수준 데이터를 리턴합니다.
    city가 None이면 두 번째 DataFrame은 빈 df로 반환.
    """
    df_prov = df[(df["구분"] == prov) & (df["시군구"].isin(["계", "합계"]))].reset_index(drop=True)
    if city:
        df_city = df[(df["구분"] == prov) & (df["시군구"] == city)].reset_index(drop=True)
    else:
        df_city = pd.DataFrame(columns=df.columns)
    return df_prov, df_city

def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: '경기도', '경기도 수원시', '경기도 수원시 영통구'")
    parser.add_argument("--start", required=True, help="시작 월 (YYYYMM)")
    parser.add_argument("--end",   required=True, help="종료 월 (YYYYMM)")
    parser.add_argument("--output", default="notsold.xlsx", help="출력 엑셀 파일명")
    args = parser.parse_args()

    prov, city = parse_region(args.region_name)
    print(f"▶ 입력 region-name: {args.region_name}")
    print(f"▶ prov_key: {prov}, city_key: {city}")

    # 데이터 수집
    df_monthly   = fetch_monthly_notsold(prov, args.start, args.end)
    df_completed = fetch_completed_notsold(prov, args.start, args.end)

    # 필터링
    m_prov, m_city = filter_region(df_monthly, prov, city)
    c_prov, c_city = filter_region(df_completed, prov, city)

    # 병합
    prov_df = pd.merge(
        m_prov[["date", "미분양호수"]],
        c_prov[["date", "완료후미분양호수"]],
        on="date", how="outer"
    )
    if city:
        city_df = pd.merge(
            m_city[["date", "미분양호수"]],
            c_city[["date", "완료후미분양호수"]],
            on="date", how="outer"
        )

    # 엑셀로 저장 (sheet 이름은 시도, 시군구)
    with pd.ExcelWriter(args.output) as writer:
        prov_df.to_excel(writer, sheet_name=prov, index=False)
        if city:
            city_df.to_excel(writer, sheet_name=city, index=False)

    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
