"""
광주 관광시설 지도 시각화 (2개 시트)
1) 구별 분포 (동구, 서구, 남구, 북구, 광산구)
2) 시설 구분별 분포 (공원, 맛집, 숙박 등)
"""

import pandas as pd
import folium
from pathlib import Path
import logging

# ============================================================
# 0. 설정
# ============================================================

INPUT_FILE = "./Major_Tourist_Attractions_in_Gwangju_FINAL.csv"
OUTPUT_MAP = "./gwangju_tourist_attractions_map_trial.html"

GWANGJU_CENTER = [35.1595, 126.8526]
ZOOM_START = 12

# 지역
DISTRICTS = ["동구", "서구", "남구", "북구", "광산구"]

# 구별 색상 (Sheet 1)
DISTRICT_COLORS = {
    "동구": "red",          # 빨강
    "서구": "blue",         # 파랑
    "남구": "green",        # 초록
    "북구": "orange",       # 주황
    "광산구": "purple",     # 보라
    "기타": "gray"          # 회색
}

# 시설 구분 색상 (Sheet 2) - 구별 색상과 겹치지 않게 조정
CATEGORY_COLORS = {
    "공원": "darkgreen",        # 진초록 (구별 초록과 구분)
    "쇼핑": "darkblue",         # 진파랑 (구별 파랑과 구분)
    "맛집": "cadetblue",        # 회청색 (구별과 다름)
    "숙박": "darkred",          # 진빨강 (구별 빨강과 구분)
    "자연": "lightgreen",       # 연초록 (구별과 다름)
    "체험/스포츠": "pink",      # 분홍 (구별 보라와 구분)
    "역사/전통": "brown",       # 갈색 (새로운 색상)
    "예술/문화": "lightblue",   # 하늘색 (구별과 다름)
    "거리": "beige",            # 베이지 (구별 회색과 구분)
    "코스관광": "black"         # 검정 (새로운 색상)
}

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
# 2. 주소에서 지역 추출
# ============================================================

def extract_district(address: str) -> str:
    """
    주소에서 광주 구(district) 추출
    형식: "광주 [구명] ..." → [구명] 추출
    """
    if not isinstance(address, str):
        return "기타"

    # 주소를 공백으로 분리
    parts = address.strip().split()

    # "광주"의 다음 부분이 구명일 가능성이 높음
    for i, part in enumerate(parts):
        if part == "광주" and i + 1 < len(parts):
            next_part = parts[i + 1]
            if next_part in DISTRICTS:
                return next_part

    # 위 방식이 안 되면 문자열 포함으로 확인
    for district in DISTRICTS:
        if district in address:
            return district

    return "기타"


# ============================================================
# 3. 지도 생성 함수
# ============================================================

def create_tourist_map(df: pd.DataFrame) -> folium.Map:
    """
    관광시설 지도 생성 (2개 시트)

    Sheet 1: 구별 분포
    Sheet 2: 시설 구분별 분포
    """

    # 지역 추출
    df['지역'] = df['주소'].apply(extract_district)

    # 지도 생성
    m = folium.Map(
        location=GWANGJU_CENTER,
        zoom_start=ZOOM_START,
        tiles="OpenStreetMap"
    )

    logger.info("  기본 지도 생성 완료")

    # ============================================================
    # SHEET 1: 구별 분포
    # ============================================================
    logger.info("\n Sheet 1: 구별 분포 생성 중...")

    district_feature_groups = {}
    district_marker_counts = {}

    for district in DISTRICTS + ["기타"]:
        fg = folium.FeatureGroup(
            name=f"[구별] {district}",
            show=True
        )
        district_feature_groups[district] = fg
        district_marker_counts[district] = 0

        dist_df = df[df['지역'] == district]

        for idx, row in dist_df.iterrows():
            if pd.isna(row['위도']) or pd.isna(row['경도']):
                continue

            color = DISTRICT_COLORS.get(district, "gray")

            popup_html = f"""
            <div style="width: 280px; font-family: Arial; font-size: 12px;">
                <h4 style="margin: 5px 0; color: #333;">{row['시설명']}</h4>
                <hr style="margin: 5px 0; border: 0.5px solid #ccc;">
                <table style="width: 100%;">
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">구분:</td>
                        <td style="padding: 3px;">{row['구분']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">지역:</td>
                        <td style="padding: 3px;">{district}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">세부:</td>
                        <td style="padding: 3px;">{row['세부구분']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">주소:</td>
                        <td style="padding: 3px;">{row['주소']}</td>
                    </tr>
                </table>
            </div>
            """

            # 마커 (정사각형)
            folium.Marker(
                location=[row['위도'], row['경도']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{row['시설명']} ({district})",
                icon=folium.Icon(
                    color=color,
                    icon_color='white',
                    icon='square',
                    prefix='fa'
                )
            ).add_to(fg)

            district_marker_counts[district] += 1

        fg.add_to(m)
        logger.info(f"   {district:10s} - {district_marker_counts[district]:3d}개")

    # ============================================================
    # SHEET 2: 시설 구분별 분포
    # ============================================================
    logger.info("\n Sheet 2: 시설 구분별 분포 생성 중...")

    category_feature_groups = {}
    category_marker_counts = {}

    for category in sorted(df['구분'].unique()):
        fg = folium.FeatureGroup(
            name=f"[시설] {category}",
            show=False  # 초기에는 숨김
        )
        category_feature_groups[category] = fg
        category_marker_counts[category] = 0

        cat_df = df[df['구분'] == category]

        for idx, row in cat_df.iterrows():
            if pd.isna(row['위도']) or pd.isna(row['경도']):
                continue

            color = CATEGORY_COLORS.get(category, "gray")
            district = row['지역']

            popup_html = f"""
            <div style="width: 280px; font-family: Arial; font-size: 12px;">
                <h4 style="margin: 5px 0; color: #333;">{row['시설명']}</h4>
                <hr style="margin: 5px 0; border: 0.5px solid #ccc;">
                <table style="width: 100%;">
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">구분:</td>
                        <td style="padding: 3px;">{row['구분']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">지역:</td>
                        <td style="padding: 3px;">{district}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">세부:</td>
                        <td style="padding: 3px;">{row['세부구분']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 3px; font-weight: bold;">주소:</td>
                        <td style="padding: 3px;">{row['주소']}</td>
                    </tr>
                </table>
            </div>
            """

            # 마커 (핀)
            folium.Marker(
                location=[row['위도'], row['경도']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{row['시설명']} ({category})",
                icon=folium.Icon(
                    color=color,
                    icon_color='white',
                    icon='info-sign',
                    prefix='glyphicon'
                )
            ).add_to(fg)

            category_marker_counts[category] += 1

        fg.add_to(m)
        logger.info(f"   {category:15s} - {category_marker_counts[category]:3d}개")

    # 범례 추가 (토글 버튼 포함)
    add_legend_with_toggle(m, district_marker_counts, category_marker_counts)

    # LayerControl 추가 (좌상단)
    folium.LayerControl(
        position='topleft',
        collapsed=False
    ).add_to(m)

    logger.info("\n 범례 및 레이어 컨트롤 추가 완료")

    return m


# ============================================================
# 4. 범례 추가 (토글 버튼)
# ============================================================

def add_legend_with_toggle(m: folium.Map, district_counts: dict, category_counts: dict) -> None:
    """
    범례 추가 (우측 하단)
    토글 버튼으로 범례 보이기/숨기기

    Args:
        m: folium.Map 객체
        district_counts: 구별 마커 개수
        category_counts: 시설 구분별 마커 개수
    """

    total_district = sum(district_counts.values())
    total_category = sum(category_counts.values())

    # 색상 HEX 변환
    color_hex_map = {
        "red": "#e74c3c",
        "blue": "#3498db",
        "green": "#2ecc71",
        "orange": "#f39c12",
        "purple": "#9b59b6",
        "gray": "#95a5a6",
        "pink": "#ff1493",
        "lightgreen": "#aed581",
        "darkred": "#8b0000",
        "darkblue": "#00008b",
        "darkgreen": "#27ae60",
        "cadetblue": "#5f9ea0",
        "brown": "#8b4513",
        "lightblue": "#87ceeb",
        "beige": "#f5f5dc",
        "black": "#2c3e50"
    }

    # 범례 HTML
    legend_html = """
    <div id="legend-container" style="position: fixed; 
                bottom: 50px; right: 50px; width: 340px; 
                background-color: white; border: 3px solid #333; 
                z-index: 9999; font-size: 12px; padding: 12px; 
                border-radius: 8px; box-shadow: 0 0 20px rgba(0,0,0,0.3);">
        
        <!-- 토글 버튼 + 탭 -->
        <div style="margin-bottom: 12px;">
            <div style="margin-bottom: 8px;">
                <button onclick="toggleLegend()" id="toggle-btn" 
                        style="width: 100%; padding: 8px; background-color: #e74c3c; 
                               color: white; border: none; border-radius: 4px; 
                               cursor: pointer; font-weight: bold;">
                    ▼ 범례 숨기기
                </button>
            </div>
            
            <div id="sheet-tabs" style="display: flex; gap: 5px;">
                <button onclick="showSheet('district')" id="tab-district" 
                        style="flex: 1; padding: 8px; background-color: #3498db; color: white; 
                               border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                     구별 분포
                </button>
                <button onclick="showSheet('category')" id="tab-category" 
                        style="flex: 1; padding: 8px; background-color: #bdc3c7; color: white; 
                               border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                     시설별 분포
                </button>
            </div>
        </div>
        
        <!-- Sheet 1: 구별 분포 -->
        <div id="district-sheet" style="display: block; max-height: 400px; overflow-y: auto;">
            <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 13px; 
                      border-bottom: 2px solid #e74c3c; padding-bottom: 5px;">
                 구별 분포 (""" + str(total_district) + """개)
            </p>
    """

    # 구별 범례
    for district in DISTRICTS + ["기타"]:
        color_name = DISTRICT_COLORS.get(district, "gray")
        color_hex = color_hex_map.get(color_name, "#666666")
        count = district_counts.get(district, 0)

        legend_html += f"""
        <p style="margin: 6px 0; padding: 5px; background-color: #f9f9f9; border-radius: 3px;">
            <span style="display: inline-block; width: 16px; height: 16px; 
                         background-color: {color_hex}; border: 2px solid #333; 
                         border-radius: 2px; margin-right: 8px; vertical-align: middle;"></span>
            <b>{district}</b>: {count}개
        </p>
        """

    # Sheet 2: 시설 구분별 분포
    legend_html += """
        </div>
        
        <!-- Sheet 2: 시설별 분포 -->
        <div id="category-sheet" style="display: none; max-height: 400px; overflow-y: auto;">
            <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 13px; 
                      border-bottom: 2px solid #27ae60; padding-bottom: 5px;">
                 시설별 분포 (""" + str(total_category) + """개)
            </p>
    """

    # 시설 구분 범례
    for category in sorted(CATEGORY_COLORS.keys()):
        color_name = CATEGORY_COLORS[category]
        color_hex = color_hex_map.get(color_name, "#666666")
        count = category_counts.get(category, 0)

        legend_html += f"""
        <p style="margin: 6px 0; padding: 5px; background-color: #f9f9f9; border-radius: 3px;">
            <span style="display: inline-block; width: 16px; height: 16px; 
                         background-color: {color_hex}; border: 2px solid #333; 
                         border-radius: 2px; margin-right: 8px; vertical-align: middle;"></span>
            <b>{category}</b>: {count}개
        </p>
        """

    # 닫기
    legend_html += """
        </div>
        
        <hr style="margin: 10px 0; border: 0.5px solid #ddd;">
        <p style="margin: 5px 0; font-size: 10px; color: #666; line-height: 1.5;">
             좌상단 <b>Layers</b> 버튼으로<br/>
            각 시설을 켜고 끌 수 있습니다.
        </p>
    </div>
    
    <script>
    function toggleLegend() {
        var sheets = document.getElementById('sheet-tabs');
        var btn = document.getElementById('toggle-btn');
        
        if (sheets.style.display === 'none') {
            sheets.style.display = 'flex';
            document.getElementById('district-sheet').style.display = 'block';
            btn.textContent = '▼ 범례 숨기기';
            btn.style.backgroundColor = '#e74c3c';
        } else {
            sheets.style.display = 'none';
            document.getElementById('district-sheet').style.display = 'none';
            document.getElementById('category-sheet').style.display = 'none';
            btn.textContent = '▶ 범례 보이기';
            btn.style.backgroundColor = '#2c3e50';
        }
    }
    
    function showSheet(sheet) {
        // 시트 숨기기
        document.getElementById('district-sheet').style.display = 'none';
        document.getElementById('category-sheet').style.display = 'none';
        
        // 버튼 스타일 초기화
        document.getElementById('tab-district').style.backgroundColor = '#bdc3c7';
        document.getElementById('tab-category').style.backgroundColor = '#bdc3c7';
        
        // 선택된 시트 표시
        if (sheet === 'district') {
            document.getElementById('district-sheet').style.display = 'block';
            document.getElementById('tab-district').style.backgroundColor = '#3498db';
        } else {
            document.getElementById('category-sheet').style.display = 'block';
            document.getElementById('tab-category').style.backgroundColor = '#27ae60';
        }
    }
    </script>
    """

    # 범례를 지도에 추가
    m.get_root().html.add_child(folium.Element(legend_html))


# ============================================================
# 5. 메인 함수
# ============================================================

def main():
    """메인 실행 함수"""

    logger.info("="*70)
    logger.info("  광주 관광시설 지도 (2개 시트: 구별/시설별 분포)")
    logger.info("="*70)

    # 1. 파일 확인
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        logger.error(f" 파일을 찾을 수 없습니다: {INPUT_FILE}")
        return

    logger.info(f"\n 입력 파일: {input_path}")

    # 2. CSV 읽기
    logger.info(" CSV 로드 중...")
    try:
        df = pd.read_csv(input_path, encoding='utf-8')
        logger.info(f" {len(df)}개 시설 로드 완료")
    except Exception as e:
        logger.error(f" CSV 읽기 실패: {e}")
        return

    # 3. 데이터 검증
    required_cols = ['시설명', '구분', '세부구분', '주소', '위도', '경도']
    for col in required_cols:
        if col not in df.columns:
            logger.error(f" '{col}' 컬럼이 없습니다")
            return

    valid_count = df[df['위도'].notna() & df['경도'].notna()].shape[0]
    logger.info(f" 위경도 있는 시설: {valid_count}개")

    # 4. 지도 생성
    logger.info("\n 지도 생성 중...")
    try:
        m = create_tourist_map(df)
    except Exception as e:
        logger.error(f" 지도 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        return

    # 5. 지도 저장
    logger.info(f"\n 지도 저장 중...")
    try:
        m.save(OUTPUT_MAP)
        logger.info(f" 저장 완료: {OUTPUT_MAP}")
    except Exception as e:
        logger.error(f" 저장 실패: {e}")
        return

    # 6. 최종 정보
    logger.info("\n" + "="*70)
    logger.info(" 지도 생성 완료!")
    logger.info("="*70)
    logger.info(f"\n 파일 위치: {Path(OUTPUT_MAP).absolute()}")
    logger.info("="*70 + "\n")


# ============================================================
# 6. 실행
# ============================================================

if __name__ == "__main__":
    main()