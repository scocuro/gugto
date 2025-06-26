# real_estate_report.py
# 공공데이터 API를 활용한 실거래 리포트 생성 스크립트

import os
import sys
import requests
import pandas as pd
from datetime import datetime
import argparse

# ── 1) 파서 설정 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='법정동코드(LAWD_CD), 예: 34110330')
group.add_argument('--region-name', help='지역명 입력(시도 또는 시군구), 예: 천안시 동남구')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도(YYYY)')
parser.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후')
parser.add_argument('--output',      default='report.xlsx', help='저장할 엑셀 파일명')
args = parser.parse_args()

# ── 2) API 키 확인 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

# ── 3) 행정안전부 법정동 코드 조회(OpenAPI) ──
REGION_API_URL = 'http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList'

def fetch_region_codes():
    try:
        params = {'serviceKey': API_KEY, 'numOfRows': 10000, 'pageNo': 1, 'resultType': 'json'}
        resp = requests.get(REGION_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        # JSON 응답 처리
        if text.startswith('{'):
            data = resp.json().get('response', {}).get('body', {}).get('items', {})
            items = data.get('item') if isinstance(data, dict) else data or []
            if items:
                return items
        # JSON 파싱 실패 또는 빈 데이터인 경우 XML 직접 파싱
        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.content)
        items = []
        # namespace 무시하고 item 태그 찾기
        for elem in root.findall('.//item'):
            rec = {}
            for child in list(elem):
                tag = child.tag
                # namespace 제거
                if '}' in tag:
                    tag = tag.split('}', 1)[1]
                rec[tag] = child.text
            items.append(rec)
        return items
    except Exception as e:
        print(f"WARNING: 지역 코드 조회 실패: {e}")
        return []

# ── 4) region_code 결정 ── ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    items = fetch_region_codes()
    if not items:
        print(f"ERROR: 지역명 '{args.region_name}' 변환을 위한 코드 목록을 가져오지 못했습니다.")
        sys.exit(1)
    # 부분 일치(normalized) 비교
    def norm(s): return s.replace(' ', '').lower()
    name_input = norm(args.region_name)
    matches = [i for i in items if name_input in norm(i.get('stdReginNm', ''))]
    if not matches:
        print(f"ERROR: '{args.region_name}'에 해당하는 코드가 없습니다.")
        sys.exit(1)
    region_code = matches[0].get('stdReginCd') or matches[0].get('sggCd') or matches[0].get('siDoCd')

start_year  = args.start_year
built_after = args.built_after
output_file = args.output

# ── 5) 거래정보 API 엔드포인트 ──
BASE_URL = (
    'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/'
    'service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
)

# ── 6) 거래 데이터 수집 함수 ──
def fetch_transactions(lawd_cd: str, deal_ym: str, page_no: int, num_of_rows: int = 1000) -> pd.DataFrame:
    try:
        params = {'serviceKey': API_KEY, 'LAWD_CD': lawd_cd, 'DEAL_YMD': deal_ym,
                  'pageNo': page_no, 'numOfRows': num_of_rows}
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        df = pd.read_xml(resp.content, xpath='//item', parser='etree')
        return df
    except Exception as e:
        print(f"WARNING: 거래 데이터 {deal_ym} 호출 실패: {e}")
        return pd.DataFrame()

# ── 7) 전체 기간 조회 ──
records = []
current_year = datetime.now().year
for year in range(start_year, current_year + 1):
    for month in range(1, 13):
        deal_ym = f"{year}{month:02d}"
        page = 1
        while True:
            df = fetch_transactions(region_code, deal_ym, page)
            if df.empty:
                break
            if '건축년도' in df.columns:
                df = df[df['건축년도'].astype(int) >= built_after]
            if df.empty:
                break
            records.append(df)
            page += 1

if not records:
    print("조건에 맞는 거래 데이터가 없습니다.")
    sys.exit(0)

# ── 8) 데이터 병합 및 가공 ──
all_data = pd.concat(records, ignore_index=True)
if '년월' in all_data.columns:
    all_data['거래년월'] = pd.to_datetime(all_data['년월'], format='%Y%m')
    all_data['year'] = all_data['거래년월'].dt.year
if {'전용면적','거래금액'}.issubset(all_data.columns):
    all_data['unit_price'] = all_data['거래금액'].astype(float) / all_data['전용면적'].astype(float)

agg = (
    all_data
    .groupby(['법정동','단지명','year'], dropna=False)
    .agg(avg_price=('거래금액','mean'), count=('거래금액','size'),
         avg_unit_price=('unit_price','mean'))
    .reset_index()
)

# ── 9) 엑셀 저장 ──
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    agg.to_excel(writer, sheet_name='연도별집계', index=False)

print(f"리포트가 '{output_file}' 로 저장되었습니다.")
