#!/usr/bin/env python3
# common.py

import os
import sys
import requests

def fetch_json_list(url, params, list_key="formList"):
    """
    MOLIT 공공데이터 API에서 페이징된 JSON 리스트를 전부 가져옵니다.
    - url: API 엔드포인트
    - params: 기본 파라미터(dict). 내부에서 API 키, pageNo, numOfRows를 추가합니다.
    - list_key: response JSON 내 실제 리스트가 들어 있는 키 (기본 'formList')
    """
    api_key = os.getenv("MOLIT_STATS_KEY")
    if not api_key:
        print("ERROR: MOLIT_STATS_KEY 환경변수를 설정하세요.")
        sys.exit(1)

    # API 키 파라미터 이름이 'key'인 경우
    params = params.copy()
    params["key"] = api_key

    items = []
    page = 1
    while True:
        params.update({
            "pageNo":    page,
            "numOfRows": 1000,
            "resultType":"json"   # JSON 응답을 기대
        })
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        recs = data.get(list_key) or []
        if not recs:
            break

        items.extend(recs)
        page += 1

    return items


def split_region_name(region_name: str):
    """
    "경기도", "경기도 수원시", "경기도 수원시 영통구" 등 1~3단계 지역명을
    (province, city, district) 튜플로 돌려줍니다. 없는 단계는 None.
    """
    parts = region_name.split()
    if len(parts) == 1:
        province = parts[0]
        city = district = None
    elif len(parts) == 2:
        province, city = parts
        district = None
    elif len(parts) >= 3:
        province, city, district = parts[0], parts[1], parts[2]
    else:
        raise ValueError("지역명은 '도', '도 시군', '도 시군 읍면동' 형식이어야 합니다.")
    return province, city, district
