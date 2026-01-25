"""
광주관광명소 데이터 출처 : https://www.data.go.kr/data/15133527/fileData.do
Kakao Local API 지오코딩 통합 스크립트

Step 1: 시설명으로 기본 검색
Step 2: 실패한 시설에 대해 텍스트 전처리 후 재검색 및 영문명 재검색

"""

import pandas as pd
import requests
import time
import logging
import re
from pathlib import Path
from typing import Dict, Optional

# ============================================================
# 0. 설정
# ============================================================

# Kakao REST API KEY (여기에 입력하세요!)
KAKAO_API_KEY = "YOUR_REST_API_KEY"

# 파일 경로
INPUT_FILE = "./Major_Tourist_Attractions_in_Gwangju_RAW.csv"  # 원본 파일
STEP1_OUTPUT_FILE = "./Major_Tourist_Attractions_in_Gwangju_STEP1.csv"  # Step 1 결과
FINAL_OUTPUT_FILE = "./Major_Tourist_Attractions_in_Gwangju_FINAL.csv"  # 최종 결과

# Kakao API
KAKAO_LOCAL_SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
REQUEST_DELAY = 0.15  # Rate limiting (약 초당 6~7개 요청)

# ============================================================
# 1. 로깅 설정
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================
# 2. 텍스트 전처리 클래스
# ============================================================

class TextPreprocessor:
    """시설명 텍스트 정제"""

    def __init__(self):
        """정제 규칙 초기화"""
        self.rules = [
            # (규칙명, 패턴, 설명)
            ("parentheses", r"\([^)]*\)", "괄호 제거: (본관, 체육관) -> 제거"),
            ("parentheses_kr", r"（[^）]*）", "전각 괄호 제거"),
            ("and_suffix", r"및 .*$", "~ 및 ~로 시작하는 뒷부분 제거"),
            ("with_suffix", r", .*$", "쉼표 뒤의 설명 제거"),
            ("building_type", r" (본관|별관|사무실|건물|홀|관|센터)", "건물 유형 제거"),
            ("number_prefix", r"^(제\d+호|NO\.\d+|[0-9]+번) ", "번호 접두사 제거"),
            ("numbers", r"\d{1,4}년?", "연도/숫자 제거"),
            ("extra_spaces", r"\s+", "여러 공백을 한 칸으로"),
        ]

    def clean(self, text: str) -> str:
        """텍스트 정제 파이프라인"""
        if not isinstance(text, str) or not text.strip():
            return ""

        result = text.strip()

        # 각 규칙 적용
        for rule_name, pattern, desc in self.rules:
            result = re.sub(pattern, " ", result)

        # 최종 정리
        result = result.strip()
        result = re.sub(r"\s+", " ", result)

        return result


# ============================================================
# 3. Kakao API 지오코딩 함수
# ============================================================

def geocode_with_kakao(facility_name: str, api_key: str) -> Dict:
    """
    Kakao Local API로 시설명 검색

    Args:
        facility_name: 시설 이름
        api_key: Kakao REST API 키

    Returns:
        {
            'address': 주소,
            'latitude': 위도,
            'longitude': 경도,
            'status': 상태 ('success', 'not_found', 'error', 'timeout'),
            'query': 검색했던 시설명
        }
    """

    if not facility_name or not isinstance(facility_name, str):
        return {
            'address': None,
            'latitude': None,
            'longitude': None,
            'status': 'invalid_input',
            'query': str(facility_name)
        }

    try:
        # API 요청 헤더
        headers = {
            "Authorization": f"KakaoAK {api_key}"
        }

        # 검색 파라미터 (광주에서만 검색하도록 지정)
        params = {
            "query": facility_name,
            "region_code": "29",  # 광주광역시 코드
            "size": 1  # 가장 상위 결과만 반환
        }

        # API 요청
        response = requests.get(
            KAKAO_LOCAL_SEARCH_URL,
            headers=headers,
            params=params,
            timeout=5
        )

        # 상태 코드 확인
        if response.status_code != 200:
            logger.warning(f"[ERROR] API 에러 ({facility_name}): {response.status_code}")
            return {
                'address': None,
                'latitude': None,
                'longitude': None,
                'status': f'api_error_{response.status_code}',
                'query': facility_name
            }

        # JSON 파싱
        data = response.json()

        # 검색 결과 확인
        if not data.get("documents") or len(data["documents"]) == 0:
            return {
                'address': None,
                'latitude': None,
                'longitude': None,
                'status': 'not_found',
                'query': facility_name
            }

        # 첫 번째 결과 추출
        result = data["documents"][0]

        return {
            'address': result.get("address_name", ""),
            'latitude': float(result.get("y", 0)),
            'longitude': float(result.get("x", 0)),
            'status': 'success',
            'query': facility_name
        }

    except requests.exceptions.Timeout:
        logger.warning(f"[TIMEOUT] 타임아웃 ({facility_name})")
        return {
            'address': None,
            'latitude': None,
            'longitude': None,
            'status': 'timeout',
            'query': facility_name
        }

    except requests.exceptions.ConnectionError:
        logger.warning(f"[NETWORK] 네트워크 에러 ({facility_name})")
        return {
            'address': None,
            'latitude': None,
            'longitude': None,
            'status': 'connection_error',
            'query': facility_name
        }

    except Exception as e:
        logger.warning(f"[ERROR] 예기치 않은 에러 ({facility_name}): {str(e)}")
        return {
            'address': None,
            'latitude': None,
            'longitude': None,
            'status': f'error:{str(e)[:20]}',
            'query': facility_name
        }


# ============================================================
# 4. Step 1: 기본 지오코딩
# ============================================================

def step1_basic_geocoding(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Step 1: 모든 시설에 대해 기본 지오코딩 수행
    """

    if not api_key or api_key == "YOUR_KAKAO_REST_API_KEY_HERE":
        logger.error("[ERROR] API 키가 설정되지 않았습니다!")
        logger.error("[ERROR] KAKAO_API_KEY에 REST API 키를 입력하세요.")
        raise ValueError("Kakao API Key not set")

    logger.info("\n" + "="*70)
    logger.info("[STEP 1] 기본 지오코딩 (시설명 검색)")
    logger.info("="*70)

    # 결과 저장할 리스트
    results = {
        'address': [],
        'latitude': [],
        'longitude': [],
        'geocoding_status': [],
        'retry_method': []
    }

    total = len(df)
    logger.info(f"[START] 시작: {total}개 시설 지오코딩")
    logger.info(f"[INFO] 추정 소요 시간: {total * REQUEST_DELAY / 60:.1f}분")

    # 각 시설별 지오코딩
    for idx, facility_name in enumerate(df['시설명'], 1):
        # 진행상황 표시
        if idx % 50 == 0 or idx == 1:
            logger.info(f"[PROGRESS] {idx}/{total} ({idx / total * 100:.1f}%)")

        # 지오코딩 수행
        result = geocode_with_kakao(facility_name, api_key)

        results['address'].append(result['address'])
        results['latitude'].append(result['latitude'])
        results['longitude'].append(result['longitude'])
        results['geocoding_status'].append(result['status'])
        results['retry_method'].append(None)  # Step 1에서는 None

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # DataFrame에 컬럼 추가
    df['주소'] = results['address']
    df['위도'] = results['latitude']
    df['경도'] = results['longitude']
    df['지오코딩상태'] = results['geocoding_status']
    df['재시도방법'] = results['retry_method']

    return df


# ============================================================
# 5. Step 2: 실패한 시설 재시도
# ============================================================

def step2_retry_failed(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Step 2: not_found 시설에 대해 텍스트 전처리 후 재검색
    """

    logger.info("\n" + "="*70)
    logger.info("[STEP 2] 실패한 시설 재시도")
    logger.info("="*70)

    preprocessor = TextPreprocessor()
    not_found_count = (df['지오코딩상태'] == 'not_found').sum()
    logger.info(f"[INFO] 재시도 대상: {not_found_count}개 시설")

    retry_count = 0

    for idx, row in df[df['지오코딩상태'] == 'not_found'].iterrows():
        facility_name = row['시설명']

        # 1) 한글명 전처리 후 재검색
        cleaned_name = preprocessor.clean(facility_name)

        if cleaned_name and cleaned_name != facility_name:
            result = geocode_with_kakao(cleaned_name, api_key)

            if result['status'] == 'success':
                df.at[idx, '주소'] = result['address']
                df.at[idx, '위도'] = result['latitude']
                df.at[idx, '경도'] = result['longitude']
                df.at[idx, '지오코딩상태'] = 'success'
                df.at[idx, '재시도방법'] = 'korean_cleaned'
                retry_count += 1
                time.sleep(REQUEST_DELAY)
                continue

            time.sleep(REQUEST_DELAY)

        # 2) 영문명 검색 (시설명_영문 컬럼이 있을 경우)
        if '시설명영문' in df.columns and pd.notna(row.get('시설명영문')):
            english_name = str(row['시설명영문']).strip()

            result = geocode_with_kakao(english_name, api_key)

            if result['status'] == 'success':
                df.at[idx, '주소'] = result['address']
                df.at[idx, '위도'] = result['latitude']
                df.at[idx, '경도'] = result['longitude']
                df.at[idx, '지오코딩상태'] = 'success'
                df.at[idx, '재시도방법'] = 'english'
                retry_count += 1
                time.sleep(REQUEST_DELAY)
                continue

            time.sleep(REQUEST_DELAY)

    logger.info(f"[SUCCESS] 재시도 성공: {retry_count}개 시설")

    return df


# ============================================================
# 6. 결과 분석 및 보고
# ============================================================

def print_geocoding_report(df: pd.DataFrame, step: str = "FINAL") -> None:
    """지오코딩 결과 보고서 출력"""

    logger.info("\n" + "="*70)
    logger.info(f"[REPORT] {step} 지오코딩 결과 리포트")
    logger.info("="*70)

    # 상태별 집계
    status_counts = df['지오코딩상태'].value_counts()

    logger.info(f"\n[RESULT] 상태별 결과:")
    for status, count in status_counts.items():
        pct = count / len(df) * 100
        logger.info(f"  {status:20s}: {count:3d}개 ({pct:5.1f}%)")

    # 성공률
    success_count = (df['지오코딩상태'] == 'success').sum()
    success_rate = success_count / len(df) * 100
    logger.info(f"\n[SUMMARY] 성공률: {success_count}/{len(df)} ({success_rate:.1f}%)")

    # 구분별 성공률
    if '구분' in df.columns:
        logger.info(f"\n[CATEGORY] 구분별 성공률:")
        for category in sorted(df['구분'].unique()):
            cat_df = df[df['구분'] == category]
            cat_success = (cat_df['지오코딩상태'] == 'success').sum()
            cat_total = len(cat_df)
            cat_rate = cat_success / cat_total * 100 if cat_total > 0 else 0
            logger.info(f"  {category:15s}: {cat_success:3d}/{cat_total:3d} ({cat_rate:5.1f}%)")

    # 실패한 시설 목록
    failed = df[df['지오코딩상태'] != 'success']
    if len(failed) > 0:
        logger.info(f"\n[FAILED] 실패한 시설 ({len(failed)}개):")
        for idx, row in failed.iterrows():
            logger.info(f"  - {row['시설명']:40s} ({row['지오코딩상태']})")

    logger.info("="*70)


# ============================================================
# 7. 메인 함수
# ============================================================

def main():
    """메인 실행 함수"""

    logger.info("\n" + "="*70)
    logger.info("[START] Kakao Local API 통합 지오코딩")
    logger.info("="*70)

    # 1. 입력 파일 확인
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        logger.error(f"[ERROR] 파일을 찾을 수 없습니다: {INPUT_FILE}")
        return

    logger.info(f"\n[FILE] 입력 파일: {input_path}")

    # 2. CSV 읽기
    logger.info("[LOAD] CSV 로드 중...")
    try:
        df = pd.read_csv(input_path, encoding='euc-kr')
        logger.info(f"[SUCCESS] {len(df)}개 시설 로드 완료")
    except Exception as e:
        logger.error(f"[ERROR] CSV 읽기 실패: {e}")
        return

    # 3. 데이터 검증
    if '시설명' not in df.columns:
        logger.error("[ERROR] '시설명' 컬럼이 없습니다")
        return

    # 4. STEP 1: 기본 지오코딩
    try:
        df = step1_basic_geocoding(df, KAKAO_API_KEY)
        logger.info("[SUCCESS] STEP 1 완료")
    except ValueError as e:
        logger.error(f"[ERROR] {e}")
        return
    except Exception as e:
        logger.error(f"[ERROR] STEP 1 중 에러: {e}")
        return

    # Step 1 중간 결과 저장
    try:
        step1_path = Path(STEP1_OUTPUT_FILE)
        df.to_csv(step1_path, index=False, encoding='utf-8-sig')
        logger.info(f"[SAVE] STEP 1 결과 저장: {step1_path}")
    except Exception as e:
        logger.warning(f"[WARNING] STEP 1 결과 저장 실패: {e}")

    # 5. STEP 2: 실패한 시설 재시도
    try:
        df = step2_retry_failed(df, KAKAO_API_KEY)
        logger.info("[SUCCESS] STEP 2 완료")
    except Exception as e:
        logger.warning(f"[WARNING] STEP 2 중 에러: {e}")

    # 6. 최종 결과 보고
    print_geocoding_report(df, "FINAL")

    # 7. 최종 결과 저장
    logger.info(f"\n[SAVE] 최종 결과 저장 중...")
    try:
        output_path = Path(FINAL_OUTPUT_FILE)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"[SUCCESS] 저장 완료: {output_path}")
    except Exception as e:
        logger.error(f"[ERROR] 저장 실패: {e}")
        return

    # 8. 최종 통계
    logger.info("\n" + "="*70)
    logger.info("[COMPLETE] 모든 작업 완료!")
    logger.info(f"  - 입력: {len(df)}개 시설")
    logger.info(f"  - 성공: {(df['지오코딩상태'] == 'success').sum()}개")
    logger.info(f"  - 성공률: {(df['지오코딩상태'] == 'success').sum() / len(df) * 100:.1f}%")
    logger.info(f"  - 출력: {output_path}")
    logger.info("="*70 + "\n")


# ============================================================
# 8. 실행
# ============================================================

if __name__ == "__main__":

    main()
