# test_region_code.py
# 행정안전부 법정동 코드 조회 API 테스트 스크립트
# API 키를 코드에 직접 삽입하여, 지역명을 입력받아 실행합니다

import sys
import requests
import xml.etree.ElementTree as ET

# 1) API 키 설정 (코드에 직접 입력)
API_KEY = "1H2R46dpQoNh8zKRKWxILkiMnfsOzc7qlHmtR+pxXQ2zRljCVoNfi03MUQTsRupsJ0MUIreOYk2QUBDjidJZGA=="

# 2) 사용자 입력: 지역명
region_name = input("조회할 지역명 입력 (예: 천안시 동남구): ").strip()
if not region_name:
    print("ERROR: 지역명을 입력해야 합니다.")
    sys.exit(1)

# 3) OpenAPI 호출 (HTTP 사용)
REGION_API_URL = 'http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList'
params = {
    'serviceKey': API_KEY,
    'numOfRows':  10000,
    'pageNo':     1,
    'resultType': 'json',
}
try:
    resp = requests.get(REGION_API_URL, params=params, timeout=30)
    resp.raise_for_status()
except Exception as e:
    print(f"ERROR: API 호출 실패: {e}")
    sys.exit(1)

# 4) JSON 파싱 시도
items = []
text = resp.text.strip()
if text.startswith('{'):
    try:
        data = resp.json()
        body = data.get('response', {}).get('body', {}).get('items', {})
        items = body.get('item') if isinstance(body, dict) else body or []
    except Exception:
        items = []

# 5) JSON이 아니거나 비어있으면 XML 파싱
if not items:
    try:
        root = ET.fromstring(resp.content)
        for elem in root.findall('.//item'):
            rec = {child.tag.split('}')[-1]: child.text for child in elem}
            items.append(rec)
    except Exception as e:
        print(f"ERROR: XML 파싱 실패: {e}")
        sys.exit(1)

# 6) 매칭 및 출력
def norm(s): return s.replace(' ', '').lower()
name_input = norm(region_name)
matches = [i for i in items if name_input in norm(i.get('stdReginNm', ''))]
if not matches:
    print(f"결과 없음: '{region_name}'에 해당하는 코드가 없습니다.")
    sys.exit(0)

print(f"'{region_name}' 검색 결과 (최대 5건):")
for m in matches[:5]:
    name = m.get('stdReginNm') or m.get('ctprvnNm') or ''
    code = m.get('stdReginCd') or m.get('sggCd') or ''
    print(f"  {name}  →  {code}")
