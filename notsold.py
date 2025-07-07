#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests
from common import get_region_code  # 기존에 쓰시던 common.py 에서 가져옵니다

# MOLIT 오픈API 기본 URL과 키
MOLIT_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
API_KEY   = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    raise EnvironmentError("환경변수 MOLIT_STATS_KEY 에 API 키를 설정해주세요")

def paginate_request(form_id: int, style_num: int, start_dt: str, end_dt: str) -> list[dict]:
    """
    MOLIT API 를 pageNo/numOfRows 페이징으로 끝까지 호출해서 item 리스트를 리턴합니다.
    """
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
    all_items = []
    while True:
        resp = requests.get(MOLIT_URL, params=params)
        resp.raise_for_status()
        body = resp.json().get("response", {}).get("body", {})
        batch = body.get("items", {}).get("item")
        if not batch:
            break
        # 단일 dict 면 리스트로 감싸기
        if isinstance(batch, dict):
            batch = [batch]
        all_items.extend(batch)
        # 마지막 페이지 체크
        if len(batch) < params["numOfRows"]:
            break
        params["pageNo"] += 1

    return all_items

def fetch_monthly_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    월별 미분양 (form_id=2082, style_num=128)
    """
    data = paginate_request(form_id=2082, style_num=128, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def fetch_completed_notsold(start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    공사완료 후 미분양 (form_id=5328, style_num=1)
    """
    data = paginate_request(form_id=5328, style_num=1, start_dt=start_dt, end_dt=end_dt)
    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def parse_region_levels(region_name: str) -> list[str]:
    """
    "경기도 수원시 영통구" → ["경기도", "경기도 수원시"]
    "경상남도 진주시"      → ["경상남도", "경상남도 진주시"]
    "충청남도"           → ["충청남도"]
    """
    parts = region_name.split()
    province = parts[0]
    levels = [province]
    if len(parts) >= 2:
        city = parts[1]
        levels.append(f"{province} {city}")
    return levels

def main():
    parser = argparse.ArgumentParser(description="미분양 현황 수집기")
    parser.add_argument(
        "--region-name", required=True,
        help="예: 경기도 수원시 영통구 (1~3단계까지 모두 지원)"
    )
    parser.add_argument("--start",  required=True, help="조회 시작 YYYYMM")
    parser.add_argument("--end",    required=True, help="조회 종료 YYYYMM")
    parser.add_argument(
        "--output", default="notsold.xlsx",
        help="출력 파일명 (엑셀, 시트마다 레벨별 데이터)"
    )
    args = parser.parse_args()

    # 입력받은 지역명을 1~2단계 레벨로 분리
    levels = parse_region_levels(args.region_name)

    # 엑셀 작성 준비
    writer = pd.ExcelWriter(args.output, engine="openpyxl")

    for lvl in levels:
        # 레벨별로 코드 확인
        code = get_region_code(lvl)
        if not code:
            raise ValueError(f"'{lvl}' 의 지역 코드를 찾을 수 없습니다. common.get_region_code 설정을 확인하세요")

        # 데이터 가져오기
        df_month = fetch_monthly_notsold(args.start, args.end)
        df_comp  = fetch_completed_notsold(args.start, args.end)

        # 반드시 '시군구' 칼럼이 있어야 필터링 가능
        if "시군구" not in df_month.columns or "시군구" not in df_comp.columns:
            raise KeyError(f"필수 컬럼 '시군구' 가 없습니다. (월별: {df_month.columns.tolist()}, 완료후: {df_comp.columns.tolist()})")

        # 레벨별(예: '경기도', '경기도 수원시') 필터링
        m = df_month[df_month["시군구"] == lvl][["date", "미분양현황"]]
        c = df_comp [df_comp ["시군구"] == lvl][["date", "미분양현황"]]

        # 칼럼명 통일
        m.columns = ["date", "monthly_notsold"]
        c.columns = ["date", "completed_notsold"]

        # 병합
        out = pd.merge(m, c, on="date", how="outer").sort_values("date")

        # 시트에 쓰기 (시트명에 공백 있으면 '_' 로 교체)
        sheet_name = lvl.replace(" ", "_")
        out.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.save()
    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
