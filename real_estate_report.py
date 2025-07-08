#!/usr/bin/env python3
# real_estate_report.py (디버그 로깅 추가)

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

# ── 2) region_code 결정 헬퍼 ──
def get_region_code(region_name: str) -> str:
    parts = region_name.split()
    print(f">>> get_region_code: 입력 region_name={region_name}")
    if len(parts) == 1:
        sido = parts[0]
        sub = csv_df[csv_df["시도명"] == sido]
    elif len(parts) == 2:
        sido, sigungu = parts
        sub = csv_df[(csv_df["시도명"] == sido) & (csv_df["시군구명"] == sigungu)]
    elif len(parts) == 3:
        sido, sigungu, eummyundong = parts
        sub = csv_df[(csv_df["시도명"] == sido)
                     & (csv_df["시군구명"] == sigungu)
                     & (csv_df["읍면동명"] == eummyundong)]
    else:
        raise ValueError("‘시도’, ‘시도 시군구’, ‘시도 시군구 읍면동’ 형식으로 입력해주세요.")

    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")

    code5 = sub.iloc[0]["법정동코드"][0:5]
    print(f">>> get_region_code: 매칭된 법정동코드(5자리)={code5}")
    return code5

# ── 3) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구[+읍면동] 명칭')
parser.add_argument('--start-year', type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 4) 시군구코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
    print(f">>> 직접 입력된 lawd_cd={region_code}")
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 5) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptSilvTrade/getRTMSDataSvcAptSilvTrade"

# ── 6) API 호출 + XML→DataFrame 헬퍼 ──
def fetch_items(url, params):
    print(f">>> fetch_items 호출: URL={url}\n    params={params}")
    try:
        r = requests.get(url, params=params, timeout=30)
        print(f">>> 응답 status_code={r.status_code}, content-type={r.headers.get('Content-Type')}\n    text[:200]={r.text[:200]!r}")
        r.raise_for_status()
    except Exception as e:
        print(f">>> 요청 에러: {e}")
        return []
    try:
        df = pd.read_xml(StringIO(r.text), xpath='.//item', parser='etree')
    except Exception as e:
        print(f">>> XML 파싱 에러: {e}")
        return []
    recs = df.to_dict(orient='records')
    print(f">>> 파싱 완료: records={len(recs)}")
    return recs

# ── 7) 전체 데이터 수집 ──
def collect_all(base_url, cols, date_key):
    today = datetime.today()
    rows = []
    for yy in range(args.start_year, today.year+1):
        max_m = today.month if yy == today.year else 12
        for mm in range(1, max_m+1):
            ymd = f"{yy}{mm:02d}"
            page = 1
            while True:
                print(f">>> collect_all: ymd={ymd}, page={page}")
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_key:     ymd,
                    'pageNo':     page,
                    'numOfRows':  1000,
                    'resultType': 'xml'
                }
                recs = fetch_items(base_url, params)
                if not recs:
                    print(f">>> collect_all: {ymd} page {page}에 더 이상 데이터 없음, 중단")
                    break
                df = pd.DataFrame(recs)
                df = df.loc[:, [*cols, 'dealYear','dealMonth','dealDay']]
                # 숫자형 변환 생략…
                rows.append(df)
                page += 1
    result = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    print(f">>> collect_all: 총 rows={len(result)}")
    return result

# ── 8) 컬럼 정의 & 수집 실행 ──
print("▶ 매매 수집…")
df_sale = collect_all(BASE_SALE_URL, ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount','buildYear'], 'DEAL_YMD')
print(f"  → 매매 {len(df_sale)}건 수집 완료")
print("▶ 전월세 수집…")
df_rent = collect_all(BASE_RENT_URL, ['sggCd','umdNm','aptNm','jibun','excluUseAr','deposit','monthlyRent','contractType'], 'DEAL_YMD')
print(f"  → 전월세 {len(df_rent)}건 수집 완료")
print("▶ 분양권 수집…")
df_silv = collect_all(BASE_SILV_URL, ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount'], 'DEAL_YMD')
print(f"  → 분양권 {len(df_silv)}건 수집 완료")

# ── 9) 엑셀 작성 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    # Raw 데이터
    df_sale.to_excel(writer, sheet_name='매매(raw)',   index=False)
    df_rent.to_excel(writer, sheet_name='전세(raw)',   index=False)
    df_silv.to_excel(writer, sheet_name='분양권(raw)', index=False)

    # 피벗 생성 함수
    def make_pivot(df, valcol):
        if df.empty:
            return pd.DataFrame()
        g = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
        pv = g.agg(
            case_count=('dealYear','size'),
            avg_value =(valcol,    'mean'),
            avg_exclu =('excluUseAr_adj','mean')
        ).reset_index()
        years = sorted(pv['dealYear'].unique())
        # 결과 틀 잡기
        out = pv[['umdNm','aptNm']].drop_duplicates().reset_index(drop=True)
        new_cols = []
        for y in years:
            new_cols += [
                f"case_count_{y}",
                f"avg_value_{y}",
                f"avg_exclu_{y}"
            ]
            sub = pv[pv['dealYear']==y]
            out[f"case_count_{y}"] = out.merge(sub[['umdNm','aptNm','case_count']],
                                              on=['umdNm','aptNm'], how='left')['case_count']
            out[f"avg_value_{y}"]  = out.merge(sub[['umdNm','aptNm','avg_value']],
                                              on=['umdNm','aptNm'], how='left')['avg_value']
            out[f"avg_exclu_{y}"]  = out.merge(sub[['umdNm','aptNm','avg_exclu']],
                                              on=['umdNm','aptNm'], how='left')['avg_exclu']
        return out[['umdNm','aptNm'] + new_cols]

    # 수정된 피벗 시트
    make_pivot(df_sale, 'dealAmount')\
        .to_excel(writer, sheet_name='매매(수정)', index=False)
    make_pivot(df_rent, 'deposit')\
        .to_excel(writer, sheet_name='전세(수정)', index=False)
    make_pivot(df_silv, 'dealAmount')\
        .to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
