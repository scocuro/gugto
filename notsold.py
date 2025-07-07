#!/usr/bin/env python3
# notsold.py

import argparse
import os
import pandas as pd
import requests

MOLIT_URL = "http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
API_KEY   = os.getenv("MOLIT_STATS_KEY")
if not API_KEY:
    raise EnvironmentError("환경변수 MOLIT_STATS_KEY 에 API 키를 설정해주세요")

def fetch_form_list(form_id: int, style_num: int, start_dt: str, end_dt: str) -> pd.DataFrame:
    """
    MOLIT API 호출해서 result_data.formList 전체를 DataFrame 으로 리턴
    """
    params = {
        "key": API_KEY,
        "form_id": form_id,
        "style_num": style_num,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    resp = requests.get(MOLIT_URL, params=params)
    resp.raise_for_status()
    js = resp.json()
    items = js["result_data"]["formList"]
    # 단일 dict 인 경우 리스트로
    if isinstance(items, dict):
        items = [items]
    df = pd.DataFrame(items)
    # 칼럼명 공백 제거
    df.columns = [c.strip() for c in df.columns]
    return df

def parse_levels(region_name: str) -> list[tuple[str, str|None]]:
    """
    "경기도 수원시 영통구" →
      [("경기도",   None),
       ("경기도", "수원시")]
    "충청남도 천안시" →
      [("충청남도", None),
       ("충청남도", "천안시")]
    "서울특별시" →
      [("서울특별시", None)]
    """
    parts = region_name.split()
    province = parts[0]
    levels = [(province, None)]
    if len(parts) >= 2:
        city = parts[1]
        levels.append((province, city))
    return levels

def main():
    p = argparse.ArgumentParser("미분양 현황 수집기")
    p.add_argument("--region-name", required=True,
                   help="예: 경기도 수원시 영통구 (1~3단계 자동 대응)")
    p.add_argument("--start", required=True, help="YYYYMM")
    p.add_argument("--end",   required=True, help="YYYYMM")
    p.add_argument("--output", default="notsold.xlsx",
                   help="엑셀 파일명")
    args = p.parse_args()

    # 1단계(시도)·2단계(시도+시군구) tuple 리스트 생성
    levels = parse_levels(args.region_name)

    # 월별(form_id=2082, style_num=128) / 완료후(form_id=5328, style_num=1) 데이터 프레임
    df_month = fetch_form_list(form_id=2082, style_num=128,
                               start_dt=args.start, end_dt=args.end)
    df_comp  = fetch_form_list(form_id=5328, style_num=1,
                               start_dt=args.start, end_dt=args.end)

    # 공통 필수 칼럼 체크
    for df in (df_month, df_comp):
        for col in ("구분", "시군구", "date", "미분양현황"):
            if col not in df.columns:
                raise KeyError(f"필수 컬럼 '{col}' 가 응답에 없습니다: {df.columns.tolist()}")

    writer = pd.ExcelWriter(args.output, engine="openpyxl")

    for prov, city in levels:
        # province 레벨 → 시도별 총계("구분"==prov & "시군구"=="계")
        # city    레벨 → 시군구별   ("구분"==prov & "시군구"==city)
        if city is None:
            mask_month = (df_month["구분"] == prov) & (df_month["시군구"] == "계")
            mask_comp  = (df_comp ["구분"] == prov) & (df_comp ["시군구"] == "계")
            sheet_name = prov.replace(" ", "_")
        else:
            mask_month = (df_month["구분"] == prov) & (df_month["시군구"] == city)
            mask_comp  = (df_comp ["구분"] == prov) & (df_comp ["시군구"] == city)
            sheet_name = f"{prov}_{city}".replace(" ", "_")

        m = df_month.loc[mask_month, ["date", "미분양현황"]].rename(
            columns={"미분양현황": "monthly_notsold"})
        c = df_comp .loc[mask_comp , ["date", "미분양현황"]].rename(
            columns={"미분양현황": "completed_notsold"})

        out = pd.merge(
            m, c, on="date", how="outer"
        ).sort_values("date")

        out.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.save()
    print(f"✅ '{args.output}' 생성 완료")

if __name__ == "__main__":
    main()
