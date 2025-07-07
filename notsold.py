#!/usr/bin/env python3
"""
미분양 현황 및 공사완료 후 미분양 현황을 조회하여 월별/완료 후 미분양 호수를 엑셀로 출력합니다.
사용 예시:
  python notsold.py --region-name "경상남도 진주시" --start 202501 --end 202505 --output notsold_report.xlsx
"""
import argparse
import os
import pandas as pd
import requests

API_KEY = os.getenv("MOLIT_STATS_KEY")
BASE_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"

# 행정구역 약어 매핑 (구분 컬럼에 사용되는 값)
PROVINCE_MAP = {
    "서울특별시": "서울", "서울": "서울",
    "부산광역시": "부산", "부산": "부산",
    "대구광역시": "대구", "대구": "대구",
    "인천광역시": "인천", "인천": "인천",
    "광주광역시": "광주", "광주": "광주",
    "대전광역시": "대전", "대전": "대전",
    "울산광역시": "울산", "울산": "울산",
    "세종특별자치시": "세종", "세종": "세종",
    "경기도": "경기", "강원도": "강원",
    "충청북도": "충북", "충청남도": "충남",
    "전라북도": "전북", "전라남도": "전남",
    "경상북도": "경북", "경상남도": "경남",
    "제주특별자치도": "제주", "제주도": "제주", "제주": "제주"
}


def normalize_province(name: str) -> str:
    """입력된 도/광역시 이름을 구분 컬럼 값에 맞는 약어로 변환"""
    return PROVINCE_MAP.get(name.strip(), name.strip())


def parse_region_name(region_name: str):
    """""
    """
    parts = region_name.strip().split()
    prov_raw = parts[0]
    prov_key = normalize_province(prov_raw)
    city_key = parts[1] if len(parts) >= 2 else None
    # 사용되지 않지만 3단계 입력 대응을 위해 파싱
    district_key = parts[2] if len(parts) >= 3 else None
    return prov_key, city_key, district_key


def fetch_data(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    """API를 호출하여 formList 데이터를 DataFrame으로 반환"""
    params = {
        "key": API_KEY,
        "form_id": form_id,
        "style_num": style_num,
        "start_dt": start_dt,
        "end_dt": end_dt,
        # 최대 건수 조회를 위한 파라미터
        "pageNo": 1,
        "numOfRows": 1000,
        "resultType": "json"
    }
    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("result_data", {}).get("formList")
    if items is None:
        raise ValueError(f"응답에 데이터가 없습니다: {data}")
    df = pd.DataFrame(items)
    return df


def filter_by_region(df: pd.DataFrame, prov_key: str, city_key: str = None):
    """주어진 DataFrame에서 province-aggregate와 city별 데이터를 반환"""
    # 컬럼명 공백 제거
    df.columns = df.columns.str.strip()
    # 필수 컬럼 체크
    for col in ["구분", "시군구"]:
        if col not in df.columns:
            raise KeyError(f"필수 컬럼 '{col}' 가 응답에 없습니다: {df.columns.tolist()}")
    # 도/광역시 단위 합계: 시군구가 '계' 혹은 '합계'
    prov_df = df[(df["구분"] == prov_key) & (df["시군구"].isin(["계", "합계"]))].copy()
    # 시/군/구 단위 (city_key이 주어졌을 때만)
    city_df = None
    if city_key:
        city_df = df[(df["구분"] == prov_key) & (df["시군구"] == city_key)].copy()
    return prov_df, city_df


def main():
    parser = argparse.ArgumentParser("미분양 현황 수집기")
    parser.add_argument("--region-name", required=True,
                        help="예: 경상남도, 경상남도 진주시, 경기도 수원시 영통구")
    parser.add_argument("--start", required=True, help="YYYYMM")
    parser.add_argument("--end",   required=True, help="YYYYMM")
    parser.add_argument("--output", default="notsold.xlsx")
    args = parser.parse_args()

    prov_key, city_key, _ = parse_region_name(args.region_name)
    print(f"▶ 입력 region-name: {args.region_name}")
    print(f"▶ prov_key: {prov_key}, city_key: {city_key}")

    # 월별 미분양 (form_id=2082, style_num=128)
    df_monthly = fetch_data(form_id=2082, style_num=128,
                             start_dt=args.start, end_dt=args.end)
    # 공사완료 후 미분양 (form_id=5328, style_num=1)
    df_completed = fetch_data(form_id=5328, style_num=1,
                               start_dt=args.start, end_dt=args.end)

    # region-level 및 city-level 필터링
    m_prov, m_city = filter_by_region(df_monthly, prov_key, city_key)
    c_prov, c_city = filter_by_region(df_completed, prov_key, city_key)

    # 컬럼명 정리
    if "미분양현황" in m_prov.columns:
        m_prov.rename(columns={"미분양현황": "월별미분양호수"}, inplace=True)
    if m_city is not None and "미분양현황" in m_city.columns:
        m_city.rename(columns={"미분양현황": "월별미분양호수"}, inplace=True)
    if "미분양현황" in c_prov.columns:
        c_prov.rename(columns={"미분양현황": "공사완료후미분양호수"}, inplace=True)
    if c_city is not None and "미분양현황" in c_city.columns:
        c_city.rename(columns={"미분양현황": "공사완료후미분양호수"}, inplace=True)

    # 날짜 기준으로 병합
    outputs = []
    # province-level
    prov_out = pd.merge(
        m_prov[["date", "월별미분양호수"]],
        c_prov[["date", "공사완료후미분양호수"]],
        on="date", how="outer"
    )
    prov_out.insert(0, "지역구분", prov_key)
    outputs.append(prov_out)
    # city-level
    if city_key and m_city is not None and c_city is not None:
        city_out = pd.merge(
            m_city[["date", "월별미분양호수"]],
            c_city[["date", "공사완료후미분양호수"]],
            on="date", how="outer"
        )
        city_out.insert(0, "지역구분", city_key)
        outputs.append(city_out)

    # 결과 합치기 및 엑셀로 저장
    if outputs:
        result_df = pd.concat(outputs, ignore_index=True)
    else:
        result_df = pd.DataFrame()
    result_df.to_excel(args.output, index=False)
    print(f"✅ '{args.output}' 생성 완료")


if __name__ == "__main__":
    main()
