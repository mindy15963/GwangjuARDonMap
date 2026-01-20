# ==========================================================
# 광주광역시 건축 관광자원
# 위치 정보 파악 + 구별 키워드 분석 시스템
# ==========================================================
# 주요 기능
# 1. CSV 데이터 로드
# 2. 주소 → 위도/경도 변환 (지오코딩)
# 3. 주소에서 '구(區)' 정보 추출
# 4. 구별 설명 키워드 빈도 분석
# 5. 지도 시각화 (위치 정보 중심)
# ==========================================================

# ----------------------------------------------------------
# 상수 및 전처리 설정
# ----------------------------------------------------------

import re
from pathlib import Path
from collections import Counter
import math
import pandas as pd
from kiwipiepy import Kiwi
from keybert import KeyBERT
import folium

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

DISTRICTS = ["동구", "서구", "남구", "북구", "광산구"]

# 더 강한 패턴 제거
RE_ORD = re.compile(r"제\s*\d+\s*(?:호|회)")
RE_YEAR = re.compile(r"\d{3,4}\s*년")
RE_NUM = re.compile(r"\b\d+(?:[.,]\d+)?\b")
RE_PUNCT = re.compile(r"[^\w\s·]")
RE_MULTI = re.compile(r"\s+")

STOPWORDS = set([
    "광주", "광주광역시", "대한민국", "국가", "등록", "등록문화재", "국가등록문화재",
    "유형문화재", "문화재자료", "기념물", "명승", "사적", "지정", "승격",
    "조선시대", "일제강점기", "근대", "현대", "개관", "준공", "완공", "증축", "중건",
    "복원", "보수", "이전", "신축", "리모델링",
    "건물", "건축", "건축물", "시설", "공간", "장소", "지역", "현재", "당시",
    "규모", "구성", "가치", "특징", "활용", "사용", "부문"
])

# ✅ Kiwi로 "명사"만 추출해서 키워드 후보를 깔끔하게 만들기
kiwi = Kiwi()

# ----------------------------------------------------------
# 1. 데이터 로드
# ----------------------------------------------------------

INPUT_CSV = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025.csv"
OUTPUT_CSV = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025_GEO.csv"
OUTPUT_MAP = "./gwangju_architecture_map.html"

# ----------------------------------------------------------
# 핵심 함수: 키워드 추출 및 분석
# ----------------------------------------------------------

def extract_district(addr: str):
    """
    주소 문자열에서 광주광역시의 '구' 정보 추출
    """
    if not isinstance(addr, str):
        return None
    for d in DISTRICTS:
        if d in addr:
            return d
    return None

def clean_text(text: str) -> str:
    """
    텍스트 전처리: 숫자, 기호, 정렬 제거
    """
    if not isinstance(text, str):
        return ""
    t = text.replace("5·18", "오월민주화")   # 의미 있는 예외는 살리기
    t = RE_ORD.sub(" ", t)
    t = RE_YEAR.sub(" ", t)
    t = RE_NUM.sub(" ", t)
    t = RE_PUNCT.sub(" ", t)
    t = RE_MULTI.sub(" ", t).strip()
    return t

def nouns_only(text: str):
    """
    Kiwi로 명사만 추출
    """
    if not text:
        return []
    tokens = []
    for token, pos, _, _ in kiwi.analyze(text)[0][0]:
        # NNG: 일반명사, NNP: 고유명사
        if pos in ("NNG", "NNP"):
            if len(token) >= 2 and token not in STOPWORDS:
                tokens.append(token)
    return tokens

def log_odds_dirichlet(one: Counter, rest: Counter, alpha=0.01, topn=30, min_count=2):
    """
    One-vs-Rest 로그 오즈 비율 계산
    """
    vocab = set(one.keys()) | set(rest.keys())
    n1, n0 = sum(one.values()), sum(rest.values())

    out = []
    V = len(vocab) if len(vocab) else 1
    for w in vocab:
        c1, c0 = one.get(w, 0), rest.get(w, 0)
        if c1 < min_count:
            continue
        p1 = (c1 + alpha) / (n1 + alpha * V)
        p0 = (c0 + alpha) / (n0 + alpha * V)
        score = math.log(p1 / (1 - p1 + 1e-12)) - math.log(p0 / (1 - p0 + 1e-12))
        out.append((w, score, c1, c0))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:topn]

def run_pipeline(csv_path: str, out_prefix="gwangju_ai", topn=30):
    """
    One-vs-Rest 파이프라인 실행
    """
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path, encoding="utf-8")

    df["DIST"] = df["ADDR"].apply(extract_district)
    df = df[df["DIST"].isin(DISTRICTS)].copy()
    df["DC_CN"] = df["DC_CN"].fillna("").map(clean_text)

    print("[구별 레코드 수]")
    print(df["DIST"].value_counts().reindex(DISTRICTS).fillna(0).astype(int).to_string())

    # 구별 토큰 카운트(명사만)
    counters = {}
    for dist in DISTRICTS:
        sub = df[df["DIST"] == dist]
        toks = []
        for s in sub["DC_CN"].tolist():
            toks.extend(nouns_only(s))
        counters[dist] = Counter(toks)

    # One-vs-Rest "차이" 키워드
    rows = []
    for dist in DISTRICTS:
        one = counters[dist]
        rest = Counter()
        for other in DISTRICTS:
            if other != dist:
                rest += counters[other]

        ranked = log_odds_dirichlet(one, rest, alpha=0.01, topn=topn, min_count=2)
        for r, (kw, score, c1, c0) in enumerate(ranked, 1):
            rows.append({
                "DIST": dist, "RANK": r, "KEYWORD": kw,
                "SCORE_LOG_ODDS": score,
                "COUNT_IN_DIST": c1, "COUNT_IN_OTHERS": c0
            })

    out = pd.DataFrame(rows).sort_values(["DIST", "RANK"])
    out_path = csv_path.parent / f"{out_prefix}_onevsrest_nouns.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n저장: {out_path}")

    # 콘솔 요약
    for dist in DISTRICTS:
        top10 = out[out["DIST"] == dist].head(10)["KEYWORD"].tolist()
        print(f"- {dist}: " + (", ".join(top10) if top10 else "(결과 없음)"))

    return out

def run_keybert_by_district(csv_path: str, out_prefix="ai_keywords", topn=30):
    """
    명사 빈도 기반 키워드 추출 (구별)
    """
    df = pd.read_csv(csv_path, encoding="utf-8")
    df["DIST"] = df["ADDR"].apply(extract_district)
    df = df[df["DIST"].isin(DISTRICTS)].copy()
    df["DC_CN"] = df["DC_CN"].fillna("").map(clean_text)

    print("[구별 레코드 수]")
    print(df["DIST"].value_counts().reindex(DISTRICTS).fillna(0).astype(int).to_string())

    # 구별로 명사만 추출하여 빈도 계산
    district_nouns = {}
    for dist in DISTRICTS:
        sub = df[df["DIST"] == dist]
        nouns = []
        for s in sub["DC_CN"].tolist():
            nouns.extend(nouns_only(s))
        # 불용어, 한 글자 제외
        nouns = [n for n in nouns if len(n) > 1 and n not in STOPWORDS]
        district_nouns[dist] = nouns

    # 각 구별로 명사 빈도순 정렬
    rows = []
    for dist in DISTRICTS:
        counter = Counter(district_nouns[dist])
        for rank, (kw, cnt) in enumerate(counter.most_common(topn), 1):
            rows.append({"DIST": dist, "RANK": rank, "KEYWORD": kw, "COUNT": cnt})

    out = pd.DataFrame(rows).sort_values(["DIST", "RANK"])
    # SCRIPT_DIR/output 하위에 저장
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "output"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / f"{out_prefix}_nouns_freq.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n저장: {out_path}")

    # 콘솔 요약
    for dist in DISTRICTS:
        sub = out[out["DIST"] == dist].head(10)
        if len(sub) == 0:
            print(f"- {dist}: (결과 없음)")
        else:
            print(f"- {dist}: " + ", ".join(sub["KEYWORD"].tolist()))

    return out

# ----------------------------------------------------------
# 1. 데이터 로드
# ----------------------------------------------------------

print("📂 CSV 파일 로딩 중...")
df = pd.read_csv(INPUT_CSV, encoding="utf-8")

# 주소가 없는 데이터는 분석 불가 → 제거
df = df.dropna(subset=["ADDR"]).reset_index(drop=True)
print(f"✅ 총 데이터 수: {len(df)}")

# ----------------------------------------------------------
# 2. 주소 → 위도/경도 변환 (지오코딩)
# ----------------------------------------------------------

print("🌍 지오코딩 설정 중...")

# OpenStreetMap 기반 지오코더 (타임아웃 10초로 설정)
geolocator = Nominatim(user_agent="gwangju_architecture_gis", timeout=10)

# 요청 속도 제한 (서버 보호 목적)
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=1,
    swallow_exceptions=True,
    max_retries=3
)

def geocode_address(address):
    """
    주소 문자열을 입력받아 위도(latitude), 경도(longitude)를 반환
    """
    try:
        location = geocode(address, timeout=10)
        if location:
            return pd.Series([location.latitude, location.longitude])
        else:
            return pd.Series([None, None])
    except Exception as e:
        print(f"  ⚠️ 지오코딩 실패: {address}")
        return pd.Series([None, None])

print("📍 주소 → 위·경도 변환 중...")
tqdm.pandas()

df[["latitude", "longitude"]] = df["ADDR"].progress_apply(geocode_address)

print("✅ 지오코딩 완료")

# ----------------------------------------------------------
# 3. 주소에서 '구(區)' 정보 추출
# ----------------------------------------------------------

df["district"] = df["ADDR"].apply(extract_district)

print("📌 구 정보 추출 완료")

# ----------------------------------------------------------
# 4. 구별 키워드 분석 (설명내용 기반)
# ----------------------------------------------------------

# 기존 전처리 함수 사용: clean_text(), nouns_only(), 등

district_keywords = {}

for district in df["district"].unique():
    texts = df[df["district"] == district]["DC_CN"].dropna()
    
    words = []
    for text in texts:
        words.extend(clean_text(text))
    
    # 단어 빈도 계산
    counter = Counter(words)
    
    # 너무 일반적인 단어 제거
    stopwords = ["있다", "이다", "한다", "있는", "대한", "위해", "관련"]
    for stopword in stopwords:
        counter.pop(stopword, None)
    
    # 상위 10개 키워드 저장
    district_keywords[district] = counter.most_common(10)

print("\n📊 구별 주요 키워드 분석 결과")
for district, keywords in district_keywords.items():
    print(f"\n[{district}]")
    for word, count in keywords:
        print(f" - {word}: {count}")

# ----------------------------------------------------------
# 5. 지도 시각화 (위치 정보 중심)
# ----------------------------------------------------------

print("\n🗺️ 지도 시각화 생성 중...")

# ----------------------------------------------------------
# 5-1. 구별 건축물 개수 통계
# ----------------------------------------------------------

district_counts = df["district"].value_counts().sort_values(ascending=False)

print("\n📊 구별 건축물 개수 현황")
print("=" * 40)
for district, count in district_counts.items():
    print(f"{district:10} : {count:3}개")
print("=" * 40)
print(f"총합: {district_counts.sum()}개")

# ----------------------------------------------------------
# 5-2. 용도별 건축물 개수 통계
# ----------------------------------------------------------

purpose_counts = df["BULD_PURPS_NM"].value_counts().sort_values(ascending=False)

print("\n📊 용도별 건축물 개수 현황")
print("=" * 50)
for purpose, count in purpose_counts.items():
    print(f"{purpose:20} : {count:3}개")
print("=" * 50)
print(f"총합: {purpose_counts.sum()}개")

# 구별 색상 지정
district_colors = {
    "동구": "blue",
    "서구": "red",
    "남구": "green",
    "북구": "purple",
    "광산구": "orange",
    "기타": "gray"
}

# 광주 중심 좌표
GWANGJU_CENTER = [35.1595, 126.8526]

m = folium.Map(
    location=GWANGJU_CENTER,
    zoom_start=12,
    tiles="OpenStreetMap"
)

for _, row in df.dropna(subset=["latitude", "longitude"]).iterrows():
    # 명사 빈도 기반 키워드 추출
    district = row['district']
    keywords_list = district_keywords.get(district, [])
    keywords_html = ""
    if keywords_list:
        keywords_text = ", ".join([kw for kw, _ in keywords_list[:5]])
        keywords_html = f"<br><br><strong>🔑 주요 키워드:</strong><br>{keywords_text}"
    
    popup_text = f"""
    <div style="font-family: Arial; width: 300px;">
        <b>{row['PLACE_NM']}</b><br>
        주소: {row['ADDR']}<br>
        구: {row['district']}<br>
        목적: {row['BULD_PURPS_NM']}<br>
        시대: {row['ERA_NM']}{keywords_html}
    </div>
    """
    
    # 구별로 다른 색상 적용
    marker_color = district_colors.get(row['district'], "gray")
    
    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=folium.Popup(popup_text, max_width=350),
        icon=folium.Icon(icon="building", prefix="fa", color=marker_color)
    ).add_to(m)

# 범례 추가 (구별 + 용도별)
legend_html = """
<div style="position: fixed; 
     bottom: 50px; right: 50px; width: 280px; height: auto; max-height: 600px;
     background-color: white; border:2px solid grey; z-index:9999; 
     font-size:13px; padding: 10px; border-radius: 5px; overflow-y: auto;">
     <p style="margin: 0 0 8px 0; font-weight: bold; border-bottom: 2px solid #ddd; padding-bottom: 5px;">🏛️ 구별 (색상)</p>
"""
for district, color in district_colors.items():
    count = district_counts.get(district, 0)
    legend_html += f'<p style="margin: 3px 0;"><i class="fa fa-map-marker" style="color:{color}"></i> {district}: {count}개</p>'
    
legend_html += """
     <p style="margin: 10px 0 8px 0; font-weight: bold; border-top: 1px solid #ddd; border-bottom: 2px solid #ddd; padding: 5px 0;">🏢 용도별</p>
"""

# 용도별 상위 10개만 범례에 표시
for purpose, count in purpose_counts.head(10).items():
    legend_html += f'<p style="margin: 3px 0;">• {purpose}: {count}개</p>'

if len(purpose_counts) > 10:
    legend_html += f'<p style="margin: 5px 0; font-style: italic; color: #666;">+ 외 {len(purpose_counts)-10}개 용도</p>'

legend_html += """
</div>
"""

m.get_root().html.add_child(folium.Element(legend_html))

m.save(OUTPUT_MAP)

print(f"✅ 지도 파일 생성 완료 → {OUTPUT_MAP}")

# ----------------------------------------------------------
# 6. 결과 CSV 저장
# ----------------------------------------------------------

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"💾 좌표 포함 CSV 저장 완료 → {OUTPUT_CSV}")

print("\n🎉 시스템 실행 완료!")
