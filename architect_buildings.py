# ==========================================================
# 광주광역시 건축물 데이터
# 지도 시각화 + 구별 색상 + (범례 클릭 -> 구별 키워드 패널)
# ==========================================================
import json
import re
import math
from pathlib import Path
from collections import Counter

import pandas as pd
import folium

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

from kiwipiepy import Kiwi


# ----------------------------------------------------------
# 0. 파일 경로/설정
# ----------------------------------------------------------
INPUT_CSV  = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025.csv"
OUTPUT_CSV = "./GT_ARCHITECTURE_TOURISM_RESOURCES_2025_GEO.csv"
OUTPUT_MAP = "./gwangju_architecture_map.html"

GWANGJU_CENTER = [35.1595, 126.8526]

DISTRICTS = ["동구", "서구", "남구", "북구", "광산구"]
DISTRICTS_WITH_ETC = DISTRICTS + ["기타"]

district_colors = {
    "동구": "blue",
    "서구": "red",
    "남구": "green",
    "북구": "purple",
    "광산구": "orange",
    "기타": "gray"
}


# ----------------------------------------------------------
# 1) 구 추출
# ----------------------------------------------------------
def extract_district(address: str) -> str:
    """
    주소에서 광주 5개 구(동/서/남/북/광산)를 찾고, 없으면 '기타'
    """
    match = re.search(r"(동구|서구|남구|북구|광산구)", str(address))
    return match.group(1) if match else "기타"


# ----------------------------------------------------------
# 2) 지오코딩 (가능하면 캐시 CSV 재사용)
# ----------------------------------------------------------
def geocode_address_factory():
    geolocator = Nominatim(user_agent="gwangju_architecture_gis", timeout=10)
    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=1,
        swallow_exceptions=True,
        max_retries=3
    )

    def geocode_address(address: str):
        try:
            loc = geocode(address, timeout=10)
            if loc:
                return pd.Series([loc.latitude, loc.longitude])
            return pd.Series([None, None])
        except Exception:
            return pd.Series([None, None])

    return geocode_address


def load_or_geocode(input_csv: str, output_csv: str) -> pd.DataFrame:
    """
    output_csv가 있으면 lat/lon 포함된 것으로 재사용.
    없으면 input_csv 읽어서 지오코딩 수행 후 output_csv로 저장.
    """
    out_path = Path(output_csv)
    if out_path.exists():
        df = pd.read_csv(out_path, encoding="utf-8-sig")
        # 위경도 컬럼이 없거나 너무 비어있으면 재계산
        if "latitude" in df.columns and "longitude" in df.columns:
            non_null = df[["latitude", "longitude"]].dropna()
            if len(non_null) > 0:
                print(f"✅ 캐시 CSV 재사용: {output_csv}")
                return df

    print("📂 CSV 로딩 + 지오코딩 수행")
    df = pd.read_csv(input_csv, encoding="utf-8")
    df = df.dropna(subset=["ADDR"]).reset_index(drop=True)

    tqdm.pandas()
    geocode_address = geocode_address_factory()
    df[["latitude", "longitude"]] = df["ADDR"].progress_apply(geocode_address)

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"💾 지오코딩 결과 저장: {output_csv}")
    return df


# ----------------------------------------------------------
# 3) 키워드 (Kiwi 명사 + One-vs-Rest log-odds)
#    - architecture_keyword.py 로직 통합
# ----------------------------------------------------------
RE_ORD   = re.compile(r"제\s*\d+\s*(?:호|회)")
RE_YEAR  = re.compile(r"\d{3,4}\s*년")
RE_NUM   = re.compile(r"\b\d+(?:[.]\d+)?\b")
RE_PUNCT = re.compile(r"[^\w\s·]")
RE_MULTI = re.compile(r"\s+")

STOPWORDS = set([
    "광주", "광주광역시", "대한민국", "국가", "등록", "등록문화재", "국가등록문화재",
    "유형문화재", "문화재자료", "기념물", "명승", "사적", "지정", "승격",
    "조선시대", "일제강점기", "근대", "현대", "개관", "준공", "완공", "증축", "중건",
    "복원", "보수", "이전", "신축", "리모델링",
    "건물", "건축", "건축물", "시설", "공간", "장소", "지역", "현재", "당시",
    "규모", "구성", "가치", "특징", "활용", "사용", "부문",
    # 추가로 너무 흔한 서술어/조사 느낌 단어
    "있다", "이다", "한다", "있는", "대한", "위해", "관련",
    # 구명
    "동구", "서구", "남구", "북구", "광산구"
])

def clean_text_for_kw(text: str) -> str:
    if not isinstance(text, str):
        return ""
    t = text.replace("5·18", "오월민주화")
    t = RE_ORD.sub(" ", t)
    t = RE_YEAR.sub(" ", t)
    t = RE_NUM.sub(" ", t)
    t = RE_PUNCT.sub(" ", t)
    t = RE_MULTI.sub(" ", t).strip()
    return t

kiwi = Kiwi()

def nouns_only(text: str):
    """
    Kiwi로 NNG/NNP만 뽑아서 키워드 후보를 정돈
    """
    if not text:
        return []
    tokens = []
    # kiwi.analyze 결과 형식: [ (tokens, score) ... ] 인데,
    # architecture_keyword.py 방식대로 첫 분석 결과만 사용 :contentReference[oaicite:3]{index=3}
    analyzed = kiwi.analyze(text)
    if not analyzed:
        return []
    for token, pos, _, _ in analyzed[0][0]:
        if pos in ("NNG", "NNP"):
            if len(token) >= 2 and token not in STOPWORDS:
                tokens.append(token)
    return tokens

def log_odds_dirichlet(one: Counter, rest: Counter, alpha=0.01, topn=20, min_count=2):
    """
    One-vs-Rest log-odds (Dirichlet smoothing)
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

def build_district_keywords(df: pd.DataFrame, topn=15, min_count=2):
    """
    df에 district, DC_CN 컬럼이 있다고 가정.
    반환: dict[district] = [{"kw":..., "cnt":..., "score":...}, ...]
    """
    # DC_CN 정리
    if "DC_CN" not in df.columns:
        df["DC_CN"] = ""

    tmp = df.copy()
    tmp["DC_CN"] = tmp["DC_CN"].fillna("").map(clean_text_for_kw)

    # 1) 구별 명사 카운트
    counters = {}
    for dist in DISTRICTS:
        sub = tmp[tmp["district"] == dist]
        toks = []
        for s in sub["DC_CN"].tolist():
            toks.extend(nouns_only(s))
        counters[dist] = Counter(toks)

    # 2) One-vs-Rest log-odds 랭킹
    result = {d: [] for d in DISTRICTS_WITH_ETC}

    for dist in DISTRICTS:
        one = counters[dist]
        rest = Counter()
        for other in DISTRICTS:
            if other != dist:
                rest += counters[other]

        ranked = log_odds_dirichlet(one, rest, alpha=0.01, topn=topn, min_count=min_count)
        # panel 표시용 payload
        result[dist] = [
            {"kw": kw, "cnt": int(c1), "score": float(score)}
            for (kw, score, c1, c0) in ranked
        ]

    # 3) '기타'는 one-vs-rest 의미가 애매해서: 그냥 빈도 TopN(명사)로 채움
    sub_etc = tmp[tmp["district"] == "기타"]
    etc_toks = []
    for s in sub_etc["DC_CN"].tolist():
        etc_toks.extend(nouns_only(s))
    etc_counter = Counter(etc_toks)
    result["기타"] = [{"kw": kw, "cnt": int(cnt), "score": 0.0} for kw, cnt in etc_counter.most_common(topn)]

    return result


# ----------------------------------------------------------
# 4) 지도 생성 + "범례 클릭 -> 키워드 패널"
# ----------------------------------------------------------
def build_map(df: pd.DataFrame, district_kw_payload: dict):
    m = folium.Map(location=GWANGJU_CENTER, zoom_start=12, tiles="OpenStreetMap")

    # 통계
    district_counts = df["district"].value_counts().sort_values(ascending=False)
    if "BULD_PURPS_NM" in df.columns:
        purpose_counts = df["BULD_PURPS_NM"].fillna("미상").value_counts().sort_values(ascending=False)
    else:
        purpose_counts = pd.Series(dtype=int)

    # -----------------------------
    # (1) 레이어(FeatureGroup) 준비
    #  - district_groups: 구별 레이어 (초기 표시)
    #  - purpose_groups:  용도별 레이어 (초기 숨김)
    # -----------------------------
    district_groups = {}
    for dist in DISTRICTS_WITH_ETC:
        fg = folium.FeatureGroup(name=f"[DIST] {dist}", show=True)  # 초기 표시
        fg.add_to(m)
        district_groups[dist] = fg

    purpose_groups = {}
    # 용도는 너무 많을 수 있어서, 일단 "전체 용도"를 만들되
    # 범례에는 Top10만 보여주고, 클릭 필터는 Top10만 대상으로 구현
    # (원하면 모든 용도도 클릭 가능하게 확장 가능)
    top_purposes = []
    if len(purpose_counts) > 0:
        top_purposes = list(purpose_counts.head(10).index)

    for purp in top_purposes:
        fg = folium.FeatureGroup(name=f"[PURP] {purp}", show=False)  # 초기 숨김
        fg.add_to(m)
        purpose_groups[purp] = fg

    # -----------------------------
    # (2) 마커 추가
    #  - 구 레이어에 1개
    #  - (Top10 용도에 해당하면) 용도 레이어에도 1개 "복제"해서 넣기
    #    -> 토글이 아주 단순해짐 (레이어 단위 add/remove)
    # -----------------------------
    for _, row in df.dropna(subset=["latitude", "longitude"]).iterrows():
        dist = row.get("district", "기타")
        purp = row.get("BULD_PURPS_NM", "미상") if "BULD_PURPS_NM" in row else "미상"
        purp = purp if isinstance(purp, str) and purp.strip() else "미상"

        marker_color = district_colors.get(dist, "gray")

        popup_text = f"""
        <div style="font-family: Arial; width: 300px;">
            <b>{row.get('PLACE_NM', '')}</b><br>
            주소: {row.get('ADDR', '')}<br>
            구: {dist}<br>
            목적: {purp}<br>
            시대: {row.get('ERA_NM', '')}
        </div>
        """

        # (A) 구 레이어 마커
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_text, max_width=350),
            icon=folium.Icon(icon="building", prefix="fa", color=marker_color),
        ).add_to(district_groups.get(dist, district_groups["기타"]))

        # (B) 용도 레이어 마커 (Top10 범위만)
        if purp in purpose_groups:
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=folium.Popup(popup_text, max_width=350),
                icon=folium.Icon(icon="building", prefix="fa", color=marker_color),
            ).add_to(purpose_groups[purp])

    # -----------------------------
    # (3) 범례 + 키워드 패널 + 토글 JS
    # -----------------------------
    kw_json = json.dumps(district_kw_payload, ensure_ascii=False)

    # Leaflet 레이어 변수명(폴리움이 만든 JS 변수명)을 얻어서 JS로 전달
    district_layer_vars = {
        dist: district_groups[dist].get_name()
        for dist in district_groups
    }
    purpose_layer_vars = {
        purp: purpose_groups[purp].get_name()
        for purp in purpose_groups
    }

    dist_layers_json = json.dumps(district_layer_vars, ensure_ascii=False)
    purp_layers_json = json.dumps(purpose_layer_vars, ensure_ascii=False)

    legend_html = f"""
    <style>
      #kw-panel {{
        position: fixed;
        bottom: 50px;
        right: 350px;
        width: 340px;
        max-height: 380px;
        background: white;
        border: 2px solid #666;
        border-radius: 10px;
        z-index: 10000;
        display: none;
        overflow: hidden;
        box-shadow: 0 6px 18px rgba(0,0,0,0.2);
        font-family: Arial;
      }}
      #kw-panel .kw-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 12px;
        border-bottom: 1px solid #ddd;
        font-weight: bold;
      }}
      #kw-panel .kw-body {{
        padding: 10px 12px;
        overflow-y: auto;
        max-height: 320px;
        font-size: 13px;
        line-height: 1.45;
      }}
      #kw-panel .kw-row {{
        padding: 6px 0;
        border-bottom: 1px dashed #eee;
      }}
      #kw-panel .kw-close {{
        cursor: pointer;
        padding: 2px 8px;
        border: 1px solid #bbb;
        border-radius: 6px;
        background: #fafafa;
        font-weight: normal;
      }}

      .legend-district a, .legend-purpose a {{
        color: inherit;
        text-decoration: none;
      }}
      .legend-district a:hover, .legend-purpose a:hover {{
        text-decoration: underline;
      }}

      .kw-sub {{
        color:#666; font-weight:normal; font-size:12px;
      }}

      .legend-chip {{
        display:inline-block;
        margin-left:6px;
        padding:1px 6px;
        border-radius:999px;
        font-size:11px;
        border:1px solid #ddd;
        color:#666;
      }}
      .legend-chip.active {{
        border-color:#333;
        color:#333;
        font-weight:bold;
      }}
    </style>

    <script>
      const DIST_KW = {kw_json};

      // folium FeatureGroup JS variable names
      const DIST_LAYERS = {dist_layers_json};   // e.g. {{ "동구": "feature_group_xxx", ... }}
      const PURP_LAYERS = {purp_layers_json};   // e.g. {{ "교육": "feature_group_yyy", ... }}

      // 현재 모드/선택 상태
      let MODE = "district";      // "district" | "purpose"
      let ACTIVE_PURPOSE = null;  // string | null

      function _getMap() {{
        // folium이 만든 map 변수는 전역에 존재 (예: map_123abc)
        // 여기선 문서 내 leaflet map 객체를 찾아오는 가장 안전한 방식:
        for (const k in window) {{
          if (k.startsWith("map_") && window[k] && window[k] instanceof L.Map) {{
            return window[k];
          }}
        }}
        return null;
      }}

      function _layerObj(varName) {{
        // varName 문자열 -> window[varName] 레이어 객체로
        return window[varName];
      }}

      function openKw(dist) {{
        const panel = document.getElementById('kw-panel');
        const title = document.getElementById('kw-title');
        const body  = document.getElementById('kw-body');

        title.textContent = dist + " 키워드";

        const items = (DIST_KW[dist] || []);
        if (items.length === 0) {{
          body.innerHTML = "<div>키워드 데이터가 없어.</div>";
        }} else {{
          body.innerHTML = items.map((x, i) => {{
            const scorePart = (x.score && x.score !== 0)
              ? `<span class="kw-sub"> | score: ${{x.score.toFixed(3)}}</span>` : "";
            return `
              <div class="kw-row">
                <b>${{i+1}}.</b> ${{x.kw}}
                <span class="kw-sub"> (count: ${{x.cnt}})</span>
                ${{scorePart}}
              </div>
            `;
          }}).join("");
        }}

        panel.style.display = "block";
      }}

      function closeKw() {{
        document.getElementById('kw-panel').style.display = "none";
      }}

      function _setLegendActivePurpose(purposeOrNull) {{
        // 범례에 active 표시 토글(칩)
        const chips = document.querySelectorAll("[data-purpose-chip]");
        chips.forEach(ch => ch.classList.remove("active"));
        if (purposeOrNull) {{
          const el = document.querySelector(`[data-purpose-chip='${{CSS.escape(purposeOrNull)}}']`);
          if (el) el.classList.add("active");
        }}
      }}

      function showDistrictMode() {{
        const map = _getMap();
        if (!map) return;

        // 모든 purpose 레이어 제거
        for (const p in PURP_LAYERS) {{
          const layer = _layerObj(PURP_LAYERS[p]);
          if (layer && map.hasLayer(layer)) map.removeLayer(layer);
        }}

        // 모든 district 레이어 추가(원래대로)
        for (const d in DIST_LAYERS) {{
          const layer = _layerObj(DIST_LAYERS[d]);
          if (layer && !map.hasLayer(layer)) map.addLayer(layer);
        }}

        MODE = "district";
        ACTIVE_PURPOSE = null;
        _setLegendActivePurpose(null);
      }}

      function togglePurpose(purp) {{
        const map = _getMap();
        if (!map) return;

        // 같은 용도를 다시 누르면 -> district 모드로 복귀
        if (MODE === "purpose" && ACTIVE_PURPOSE === purp) {{
          showDistrictMode();
          return;
        }}

        // purpose 모드로 전환:
        // 1) district 레이어 모두 제거
        for (const d in DIST_LAYERS) {{
          const layer = _layerObj(DIST_LAYERS[d]);
          if (layer && map.hasLayer(layer)) map.removeLayer(layer);
        }}

        // 2) purpose 레이어 전부 제거 후, 선택 레이어만 추가
        for (const p in PURP_LAYERS) {{
          const layer = _layerObj(PURP_LAYERS[p]);
          if (layer && map.hasLayer(layer)) map.removeLayer(layer);
        }}

        const chosen = _layerObj(PURP_LAYERS[purp]);
        if (chosen) map.addLayer(chosen);

        MODE = "purpose";
        ACTIVE_PURPOSE = purp;
        _setLegendActivePurpose(purp);
      }}

      // 초기 로딩 시: purpose 레이어는 숨김 보장
      document.addEventListener("DOMContentLoaded", () => {{
        const map = _getMap();
        if (!map) return;
        for (const p in PURP_LAYERS) {{
          const layer = _layerObj(PURP_LAYERS[p]);
          if (layer && map.hasLayer(layer)) map.removeLayer(layer);
        }}
      }});
    </script>

    <div id="kw-panel">
      <div class="kw-header">
        <div id="kw-title">키워드</div>
        <div class="kw-close" onclick="closeKw()">닫기</div>
      </div>
      <div class="kw-body" id="kw-body"></div>
    </div>

    <div style="position: fixed;
         bottom: 50px; right: 50px; width: 280px; height: auto; max-height: 600px;
         background-color: white; border:2px solid grey; z-index:9999;
         font-size:13px; padding: 10px; border-radius: 8px; overflow-y: auto;">
         <p style="margin: 0 0 8px 0; font-weight: bold; border-bottom: 2px solid #ddd; padding-bottom: 5px;">🏛️ 구별 (색상)</p>
    """

    # 구별 항목(클릭=키워드 패널)
    for dist, color in district_colors.items():
        cnt = int(district_counts.get(dist, 0))
        legend_html += f"""
          <p class="legend-district" style="margin: 3px 0;">
            <a href="#" onclick="openKw('{dist}'); return false;">
              <i class="fa fa-map-marker" style="color:{color}"></i> {dist}: {cnt}개
            </a>
          </p>
        """

    legend_html += """
         <p style="margin: 10px 0 8px 0; font-weight: bold; border-top: 1px solid #ddd; border-bottom: 2px solid #ddd; padding: 5px 0;">
           🏢 용도별 (클릭=필터 / 다시 클릭=구별 복귀)
         </p>
    """

    # 용도 Top10 항목(클릭=필터 토글)
    if len(purpose_counts) > 0:
        for purp, cnt in purpose_counts.head(10).items():
            purp_str = str(purp)
            legend_html += f"""
              <p class="legend-purpose" style="margin: 3px 0;">
                <a href="#" onclick="togglePurpose('{purp_str}'); return false;">
                  • {purp_str}: {int(cnt)}개
                  <span class="legend-chip" data-purpose-chip="{purp_str}">active</span>
                </a>
              </p>
            """
        if len(purpose_counts) > 10:
            legend_html += f'<p style="margin: 5px 0; font-style: italic; color: #666;">+ 외 {len(purpose_counts)-10}개 용도</p>'
    else:
        legend_html += '<p style="margin: 3px 0; color:#666;">(용도 컬럼이 없거나 비어있음)</p>'

    legend_html += "</div>"

    m.get_root().html.add_child(folium.Element(legend_html))
    return m


# ----------------------------------------------------------
# 5) 실행
# ----------------------------------------------------------
def main():
    # (A) 데이터 로드 + 지오코딩(캐시 재사용)
    df = load_or_geocode(INPUT_CSV, OUTPUT_CSV)

    # (B) district 컬럼 만들기
    df["district"] = df["ADDR"].apply(extract_district)

    # (C) 구별 키워드 만들기
    print("\n🧠 구별 키워드(명사+one-vs-rest) 계산 중...")
    district_kw_payload = build_district_keywords(df, topn=15, min_count=2)

    # (D) 지도 만들기
    print("\n🗺️ 지도 생성 중...")
    m = build_map(df, district_kw_payload)

    # (E) 저장
    m.save(OUTPUT_MAP)
    print(f"✅ 지도 파일 저장 완료 → {OUTPUT_MAP}")

    # 필요하면 df도 저장(지오코딩 캐시 목적)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"💾 CSV 저장 완료 → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
