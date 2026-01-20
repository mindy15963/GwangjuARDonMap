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

import pandas as pd
import folium
import re
from collections import Counter

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

# ----------------------------------------------------------
# 1. 데이터 로드
# ----------------------------------------------------------

INPUT_CSV = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025.csv"
OUTPUT_CSV = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025_GEO.csv"
OUTPUT_MAP = "./gwangju_architecture_map.html"

print("📂 CSV 파일 로딩 중...")
df = pd.read_csv(INPUT_CSV, encoding="utf-8")

# 주소가 없는 데이터는 분석 불가 → 제거
df = df.dropna(subset=["ADDR"]).reset_index(drop=True)
print(f"✅ 총 데이터 수: {len(df)}")

# ----------------------------------------------------------
# 2. 주소 → 위도/경도 변환 (지오코딩)
# ----------------------------------------------------------

print("🌍 지오코딩 설정 중...")

# OpenStreetMap 기반 지오코더
geolocator = Nominatim(user_agent="gwangju_architecture_gis")

# 요청 속도 제한 (서버 보호 목적)
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=1,
    swallow_exceptions=True
)

def geocode_address(address):
    """
    주소 문자열을 입력받아 위도(latitude), 경도(longitude)를 반환
    """
    try:
        location = geocode(address)
        if location:
            return pd.Series([location.latitude, location.longitude])
        else:
            return pd.Series([None, None])
    except:
        return pd.Series([None, None])

print("📍 주소 → 위·경도 변환 중...")
tqdm.pandas()

df[["latitude", "longitude"]] = df["ADDR"].progress_apply(geocode_address)

print("✅ 지오코딩 완료")

# ----------------------------------------------------------
# 3. 주소에서 '구(區)' 정보 추출
# ----------------------------------------------------------

def extract_district(address):
    """
    주소 문자열에서 광주광역시의 '구' 정보 추출
    예: 광주광역시 동구 ○○로 → 동구
    """
    match = re.search(r"(동구|서구|남구|북구|광산구)", str(address))
    if match:
        return match.group(1)
    return "기타"

df["district"] = df["ADDR"].apply(extract_district)

print("📌 구 정보 추출 완료")

# ----------------------------------------------------------
# 4. 구별 키워드 분석 (설명내용 기반)
# ----------------------------------------------------------

def clean_text(text):
    """
    한글만 남기고 불필요한 기호 제거
    """
    text = str(text)
    text = re.sub(r"[^가-힣\s]", "", text)
    return text.split()

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
    popup_text = f"""
    <div style="font-family: Arial; width: 300px;">
        <b>{row['PLACE_NM']}</b><br>
        주소: {row['ADDR']}<br>
        구: {row['district']}<br>
        목적: {row['BULD_PURPS_NM']}<br>
        시대: {row['ERA_NM']}
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
