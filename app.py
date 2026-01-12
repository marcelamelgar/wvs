#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import unicodedata
from pathlib import Path
from typing import List
from urllib.parse import quote_plus, unquote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ==========================================
# CONFIGURACIÓN GENERAL DEL DASHBOARD
# ==========================================

st.set_page_config(
    page_title="WVS Guatemala",
    page_icon="📊",
    layout="wide",
)

GEOJSON_PATH = Path(__file__).parent / "mapita.geojson"

# ==========================================
# ESTILOS PERSONALIZADOS (CSS)
# ==========================================

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap');
@import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css');

body, [class^="css"] {
    font-family: "Poppins", sans-serif;
}

/* Fondo */
.stApp {
    background: radial-gradient(circle at top left, #e0f2fe 0, #f5f7fb 40%, #ffffff 80%);
}

/* ================== TÍTULOS ================== */

.main-title {
    font-size: 2.8rem;
    font-weight: 800;
    background: linear-gradient(90deg, #1d4ed8, #06b6d4);
    -webkit-background-clip: text;
    color: transparent;
    margin-bottom: 0.5rem;
}

.subtitle {
    font-size: 1.1rem;
    color: #475569;
    margin-bottom: 2rem;
}

/* ================== TARJETAS NIVEL 1 (AGRUPACIONES) ================== */

.wvs-card,
.wvs-card:link,
.wvs-card:visited {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;

    height: 150px;
    width: 100%;
    border-radius: 20px;

    text-decoration: none;
    background-color: #16324f;
    color: #ffffff !important;
    font-weight: 600;

    padding: 1.6rem 1rem;
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.25);
    transition: all 0.18s ease-in-out;

    margin-bottom: 2.2rem !important;
}

.wvs-card:hover {
    background-color: #81c3d7;
    color: #16324f !important;
    transform: translateY(-4px);
    box-shadow: 0 16px 30px rgba(15, 23, 42, 0.35);
}

/* ICONOS GRANDES */
.wvs-card-icon i {
    font-size: 4.6rem;
    margin-bottom: 0.6rem;
}

/* TEXTO TARJETA */
.wvs-card-text {
    font-size: 1.35rem !important;
    line-height: 1.4;
    color: inherit !important;
}

/* ================== TARJETAS NIVEL 2 (CATEGORÍAS) ================== */

.wvs-card-cat,
.wvs-card-cat:link,
.wvs-card-cat:visited {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;

    height: 130px;
    width: 100%;
    border-radius: 18px;

    background-color: #16324f;
    color: #ffffff;
    font-weight: 500;

    padding: 1.2rem 0.8rem;
    box-shadow: 0 8px 20px rgba(15, 23, 42, 0.22);
    transition: all 0.18s ease-in-out;

    margin-bottom: 1.6rem;
    text-decoration: none;
}

.wvs-card-cat:hover {
    background-color: #81c3d7;
    color: #16324f;
    transform: translateY(-3px);
    box-shadow: 0 14px 26px rgba(15, 23, 42, 0.32);
}

.wvs-card-cat-icon i {
    font-size: 3rem;
    margin-bottom: 0.5rem;
}

.wvs-card-cat-text {
    font-size: 1.1rem;
    line-height: 1.35;
}

.section-divider {
    border-top: 1px solid #cbd5e1;
    margin: 1.8rem 0;
}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ==========================================
# CONEXIÓN A POSTGRES
# ==========================================

# CONEXIÓN A POSTGRES (NEON + secrets)

PCT_MIN_2025 = 0.5
PG_SCHEMA = "WVS"
PG_TABLE  = "encuestas_2020_2025"

def get_engine():
    url = st.secrets["postgres"]["url"]
    return create_engine(url)

engine = get_engine()



# columnas
COL_RESPONDENT = "respondent_id"
COL_YEAR       = "year"
COL_QID        = "question_id"
COL_RESPUESTA  = "respuesta"
COL_LABEL_EN   = "label_en"
COL_COL_2020   = "col_name_2020"
COL_LABEL_ES   = "label_es"
COL_CATEGORIA  = "categoria"
COL_ESPECIF    = "especificacion"
COL_DEPTO      = "departamento"
COL_MUNI       = "municipio"
COL_AGRUP      = "agrupacion"

# ==========================================
# CONSULTAS A POSTGRES
# ==========================================

def has_data_for_categoria(agrupacion: str, categoria: str) -> bool:
    q = text(f"""
        SELECT 1
        FROM "{PG_SCHEMA}".{PG_TABLE}
        WHERE {COL_AGRUP} = :agr
          AND {COL_CATEGORIA} = :cat
          AND {COL_RESPUESTA} IS NOT NULL
          AND {COL_YEAR} IN (2020, 2025)
        LIMIT 1;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"agr": agrupacion, "cat": categoria})
    return not df.empty

def load_agrupaciones() -> List[str]:
    query = text(f"""
        SELECT DISTINCT {COL_AGRUP} AS agrupacion
        FROM "{PG_SCHEMA}".{PG_TABLE}
        WHERE {COL_AGRUP} IS NOT NULL
        ORDER BY agrupacion;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["agrupacion"].dropna().tolist()

def load_categorias(agrupacion: str) -> List[str]:
    """
    Devuelve las categorías limpias (sin espacios, sin vacías, sin 'none')
    y solo aquellas que tienen datos.
    """
    query = text(f"""
        SELECT DISTINCT {COL_CATEGORIA} AS categoria
        FROM "{PG_SCHEMA}".{PG_TABLE}
        WHERE {COL_AGRUP} = :agr
          AND {COL_CATEGORIA} IS NOT NULL
        ORDER BY categoria;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"agr": agrupacion})

    if df.empty:
        return []

    cats_raw = (
        df["categoria"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    cats_raw = cats_raw[
        (cats_raw != "") &
        (~cats_raw.str.lower().isin(["none", "nan"]))
    ]

    cats = cats_raw.unique().tolist()

    # filtramos las que no tienen datos reales
    cats = [c for c in cats if has_data_for_categoria(agrupacion, c)]
    return cats

# ==========================================
# UTILIDADES % / MULTI-RESPUESTAS
# ==========================================

def fmt_pct(x: float) -> float:
    if pd.isna(x):
        return 0.0
    return round(float(x), 2)

def explode_multiselect(df: pd.DataFrame, col: str = COL_RESPUESTA) -> pd.DataFrame:
    df = df.copy()
    df[col] = df[col].astype(str).str.replace(";", ",")
    mask_multi = df[col].str.contains(",", na=False)

    df_single = df[~mask_multi].copy()
    df_multi  = df[mask_multi].copy()

    df_multi[col] = df_multi[col].str.split(",")
    df_multi = df_multi.explode(col)
    df_multi[col] = df_multi[col].str.strip()

    df_out = pd.concat([df_single, df_multi], ignore_index=True)
    df_out = df_out[df_out[col] != ""]
    return df_out

def summarize_by_year(df: pd.DataFrame, col: str = COL_RESPUESTA) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[COL_YEAR, col, "n", "pct"])

    out = (
        df.groupby([COL_YEAR, col])
        .size()
        .reset_index(name="n")
    )
    out["total_year"] = out.groupby(COL_YEAR)["n"].transform("sum")
    out["pct"] = out["n"] / out["total_year"] * 100
    out["pct"] = out["pct"].apply(fmt_pct)
    return out



# ========= NUEVO: unificar respuestas numéricas + texto usando label_es =========

def normalize_respuesta_using_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si la respuesta es solo un número y label_es empieza con ese número,
    usamos label_es como respuesta. Así se juntan '1' y
    '1 Completamente insatisfecho' en una sola categoría.
    """
    df = df.copy()
    if COL_RESPUESTA not in df.columns:
        return df

    if COL_LABEL_ES not in df.columns:
        df[COL_RESPUESTA] = df[COL_RESPUESTA].astype(str).str.strip()
        return df

    def _norm(row):
        resp = "" if pd.isna(row[COL_RESPUESTA]) else str(row[COL_RESPUESTA]).strip()
        label = "" if pd.isna(row[COL_LABEL_ES]) else str(row[COL_LABEL_ES]).strip()

        # respuesta es numérica y label_es empieza con ese número
        if resp and resp.isdigit() and label:
            if label.startswith(resp + " ") or label == resp:
                return label

        # respuesta vacía pero label_es sí tiene algo
        if (resp == "" or resp.lower() in ("nan", "none")) and label:
            return label

        return resp

    df[COL_RESPUESTA] = df.apply(_norm, axis=1)
    df[COL_RESPUESTA] = df[COL_RESPUESTA].astype(str).str.strip()
    return df

# ==========================================
# MAPAS
# ==========================================

def normalizar_nombre(s: str) -> str:
    if s is None:
        return None
    s = str(s).strip().upper()
    s = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    while "  " in s:
        s = s.replace("  ", " ")
    return s

#@st.cache_data(show_spinner=False)
def load_guate_geojson():
    if not GEOJSON_PATH.exists():
        return None
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)
    for feat in gj.get("features", []):
        props = feat.get("properties", {})
        name = props.get("NAME_1")
        props["NAME_STD"] = normalizar_nombre(name)
    return gj

def build_depto_map_df(
    df_map: pd.DataFrame,
    col_depto: str,
    col_resp: str,
    resp_value: str,
    guate_geo: dict,
) -> pd.DataFrame:
    if df_map.empty or col_depto is None:
        return pd.DataFrame()

    df_map = df_map.copy()
    df_map["respuesta_norm"] = df_map[col_resp].astype(str).str.strip()
    df_map["depto_norm"] = df_map[col_depto].apply(normalizar_nombre)

    totals_dep = (
        df_map
        .groupby("depto_norm")
        .size()
        .reset_index(name="total_dep")
    )

    df_resp = df_map[df_map["respuesta_norm"] == resp_value].copy()
    counts_resp = (
        df_resp
        .groupby("depto_norm")
        .size()
        .reset_index(name="n")
    )

    summary_dep = totals_dep.merge(counts_resp, on="depto_norm", how="left")
    summary_dep["n"] = summary_dep["n"].fillna(0)
    summary_dep["pct"] = summary_dep["n"] / summary_dep["total_dep"] * 100

    first_names = (
        df_map
        .groupby("depto_norm")[col_depto]
        .first()
        .reset_index()
    )
    summary_dep = summary_dep.merge(first_names, on="depto_norm", how="left")

    rows_geo = []
    for feat in guate_geo.get("features", []):
        props = feat.get("properties", {})
        rows_geo.append({
            "depto_norm": props.get("NAME_STD"),
            "depto_geo": props.get("NAME_1"),
        })
    geo_df = pd.DataFrame(rows_geo)

    full_map = geo_df.merge(summary_dep, on="depto_norm", how="left")
    full_map["label_depto"] = full_map[col_depto].fillna(full_map["depto_geo"])
    full_map["n"] = full_map["n"].fillna(0)
    full_map["total_dep"] = full_map["total_dep"].fillna(0)
    full_map["pct"] = full_map["pct"].fillna(0)
    full_map["pct"] = full_map["pct"].apply(fmt_pct)
    return full_map

# ==========================================
# ICONOS
# ==========================================

def fa_icon_for_group(agr: str) -> str:
    s = agr.lower()
    if "demographic" in s: return "fa-solid fa-users"
    if "economic" in s: return "fa-solid fa-coins"
    if "ethical" in s: return "fa-solid fa-scale-balanced"
    if "happiness" in s or "wellbeing" in s: return "fa-solid fa-face-smile-beam"
    if "postmaterialism" in s: return "fa-solid fa-seedling"
    if "science" in s or "technology" in s: return "fa-solid fa-flask"
    if "corruption" in s: return "fa-solid fa-triangle-exclamation"
    if "migration" in s: return "fa-solid fa-plane-departure"
    if "security" in s: return "fa-solid fa-shield-halved"
    if "political culture" in s or "regimes" in s: return "fa-solid fa-landmark"
    if "political interest" in s or "participation" in s: return "fa-solid fa-person-chalkboard"
    if "religious" in s: return "fa-solid fa-church"
    if "social capital" in s or "organizational membership" in s: return "fa-solid fa-handshake"
    if "stereotypes" in s or "norms" in s: return "fa-solid fa-brain"
    return "fa-solid fa-chart-line"

# ==========================================
# ICONOS POR CATEGORÍA DENTRO DE CADA AGRUPACIÓN
# (diccionario igual que el tuyo)
# ==========================================

CATEGORY_ICON_MAP = {
    # ---------------- Demographic and Socioeconomic ----------------
    "Demographic and Socioeconomic": {
        "Are you the chief wage earner in your house": "fa-solid fa-money-bill-wave",
        "Do you live with your parents": "fa-solid fa-house-chimney-user",
        "Employment status": "fa-solid fa-briefcase",
        "Employment status - Respondent's Spouse": "fa-solid fa-people-arrows",
        "Ethnic group": "fa-solid fa-people-group",
        "Family savings during past year": "fa-solid fa-piggy-bank",
        "Father immigrant": "fa-solid fa-person-walking-luggage",
        "Highest educational level": "fa-solid fa-graduation-cap",
        "How many children do you have": "fa-solid fa-children",
        "Language at home": "fa-solid fa-language",
        "Mother immigrant": "fa-solid fa-person-walking-luggage",
        "Number of people in household": "fa-solid fa-people-roof",
        "Religious denomination - detailed list": "fa-solid fa-church",
        "Religious denominations - major groups": "fa-solid fa-cross",
        "Respondent citizen": "fa-solid fa-id-card",
        "Respondent immigrant": "fa-solid fa-plane-arrival",
        "Respondent - Occupational group": "fa-solid fa-user-tie",
        "Scale of incomes": "fa-solid fa-scale-balanced",
        "Sector of employment": "fa-solid fa-industry",
        "Social class (subjective)": "fa-solid fa-house-flag",
    },

    # ---------------- Economic Values ----------------
    "Economic Values": {
        "Competition good or harmful": "fa-solid fa-scale-balanced",
        "Government's vs individual's responsibility": "fa-solid fa-people-arrows-left-right",
        "Incomes should be made more equal vs There should be greater incentives for individual effort": "fa-solid fa-arrows-left-right",
        "Private vs state ownership of business": "fa-solid fa-building-columns",
        "Protecting environment vs. Economic growth": "fa-solid fa-leaf",
        "Success": "fa-solid fa-trophy",
    },

    # ---------------- Ethical Values ----------------
    "Ethical Values": {
        "Degree of agreement": "fa-solid fa-thumbs-up",
        "Government has the right": "fa-solid fa-gavel",
        "Justifiable": "fa-solid fa-scale-balanced",
    },

    # ---------------- Happiness and Wellbeing ----------------
    "Happiness and Wellbeing": {
        "Feeling of happiness": "fa-solid fa-face-grin-beam",
        "Frequency you/family (last 12 month)": "fa-solid fa-people-roof",
        "In the last 12 month, how often have you or your family": "fa-solid fa-calendar-day",
        "Satisfaction with financial situation of household": "fa-solid fa-piggy-bank",
        "Satisfaction with your life": "fa-solid fa-heart",
        "Standard of living comparing with your parents": "fa-solid fa-person-arrow-up-from-line",
        "State of health (subjective)": "fa-solid fa-heart-pulse",
    },

    # ---------------- Index of Postmaterialism ----------------
    "Index of Postmaterialism": {
        "Aims of country": "fa-solid fa-flag",
        "Aims of respondent": "fa-solid fa-user-astronaut",
        "Most important": "fa-solid fa-star",
    },

    # ---------------- Perceptions about Science and Technology ----------------
    "Perceptions about Science and Technology": {
        "Because of science and technology, there will be more opportunities for the next generation": "fa-solid fa-rocket",
        "It is not important for me to know about science in my daily life": "fa-solid fa-book-open",
        "One of the bad effects of science is that it breaks down people's ideas of right and wrong": "fa-solid fa-flask-vial",
        "Science and technology are making our lives healthier, easier, and more comfortable": "fa-solid fa-microscope",
        "The world is better off, or worse off, because of science and technology": "fa-solid fa-earth-americas",
        "We depend too much on science and not enough on faith": "fa-solid fa-scale-unbalanced",
    },

    # ---------------- Perceptions of Corruption ----------------
    "Perceptions of Corruption": {
        "Degree of agreement": "fa-solid fa-comment-dots",
        "Frequency ordinary people pay a bribe, give a gift or do a favor to local officials/service providers in order to get services": "fa-solid fa-hand-holding-dollar",
        "Involved in corruption": "fa-solid fa-user-secret",
        "Perceptions of corruption in the country": "fa-solid fa-city",
        "Risk to be held accountable for giving or receiving a bribe": "fa-solid fa-scale-balanced",
    },

    # ---------------- Perceptions of Migration ----------------
    "Perceptions of Migration": {
        "Immigration in your country": "fa-solid fa-person-walking-luggage",
        "Immigration policy preference": "fa-solid fa-passport",
        "Impact of immigrants on the development of the country": "fa-solid fa-globe",
    },

    # ---------------- Perceptions of Security ----------------
    "Perceptions of Security": {
        "Freedom and Equality - Which more important": "fa-solid fa-scale-balanced",
        "Freedom and security - Which more important": "fa-solid fa-scale-unbalanced-flip",
        "Frequency in your neighborhood": "fa-solid fa-house-chimney",
        "Respondent was victim of a crime during the past year": "fa-solid fa-person-falling-burst",
        "Respondent's family was victim of a crime during last year": "fa-solid fa-house-crack",
        "Secure in neighborhood": "fa-solid fa-shield-halved",
        "Things done for reasons of security": "fa-solid fa-camera-cctv",
        "Willingness to fight for country": "fa-solid fa-person-military-to-person",
        "Worries": "fa-solid fa-face-frown-open",
    },

    # ---------------- Political Culture and Political Regimes ----------------
    "Political Culture and Political Regimes": {
        "Democracy": "fa-solid fa-landmark-flag",
        "Feel close to the world": "fa-solid fa-earth-europe",
        "Feel close to your continent": "fa-solid fa-earth-africa",
        "Feel close to your country": "fa-solid fa-flag",
        "Feel close to your district, region": "fa-solid fa-location-dot",
        "Feel close to your village, town or city": "fa-solid fa-city",
        "How democratically is this country being governed today": "fa-solid fa-scale-balanced",
        "Importance of democracy": "fa-solid fa-check-double",
        "Left-right political scale": "fa-solid fa-arrows-left-right",
        "National pride": "fa-solid fa-flag-usa",
        "Political system": "fa-solid fa-diagram-project",
        "Respect for individual human rights nowadays": "fa-solid fa-handshake-angle",
        "Satisfaction with the political system performance": "fa-solid fa-face-smile",
    },

    # ---------------- Political Interest and Political Participation ----------------
    "Political Interest and Political Participation": {
        "How much would you say the political system in your country allows people like you to have a say in what the government does?": "fa-solid fa-people-group",
        "How often discusses political matters with friends": "fa-solid fa-comments",
        "How often in country's elections": "fa-solid fa-calendar-check",
        "Information source": "fa-solid fa-newspaper",
        "Interest in politics": "fa-solid fa-lightbulb",
        "Political action": "fa-solid fa-hand-fist",
        "Political actions online": "fa-solid fa-wifi",
        "Social activism": "fa-solid fa-people-carry-box",
        "Some people think that having honest elections makes a lot of difference in their lives; other people think that it doesn't matter much": "fa-solid fa-person-booth",
        "Vote in elections": "fa-solid fa-square-poll-vertical",
        "Which party would you vote for if there were a national election tomorrow": "fa-solid fa-square-check",
    },

    # ---------------- Religious Values ----------------
    "Religious Values": {
        "Believe in": "fa-solid fa-hands-praying",
        "How often do you attend religious services": "fa-solid fa-church",
        "How often do you pray": "fa-solid fa-person-praying",
        "Importance of God": "fa-solid fa-star-of-david",
        "Meaning of religion": "fa-solid fa-book-open-reader",
        "Religious person": "fa-solid fa-user",
        "The only acceptable religion is my religion": "fa-solid fa-ban",
        "Whenever science and religion conflict, religion is always right": "fa-solid fa-scale-balanced",
    },

    # ---------------- Social Capital, Trust and Organizational Membership ----------------
    "Social Capital, Trust and Organizational Membership": {
        "Active/Inactive membership": "fa-solid fa-users-gear",
        "Confidence": "fa-solid fa-thumbs-up",
        "Countries with the permanent seats on the UN Security Council": "fa-solid fa-earth-americas",
        "International organizations": "fa-solid fa-building-columns",
        "Most people can be trusted": "fa-solid fa-handshake",
        "Trust": "fa-solid fa-handshake-angle",
        "Where are the headquarters of the International Monetary Fund (IMF) located?": "fa-solid fa-building-flag",
        "Which of the following problems does the organization Amnesty International deal with?": "fa-solid fa-scale-balanced",
    },

    # ---------------- Social Values, Norms, Stereotypes ----------------
    "Social Values, Norms, Stereotypes": {
        "Basic kinds of attitudes concerning society": "fa-solid fa-circle-nodes",
        "Being a housewife just as fulfilling": "fa-solid fa-person-dress",
        "Duty towards society to have children": "fa-solid fa-children",
        "Future changes": "fa-solid fa-forward-fast",
        "Homosexual couples are as good parents as other couples": "fa-solid fa-people-arrows-left-right",
        "Important child qualities": "fa-solid fa-child-reaching",
        "Important in life": "fa-solid fa-heart-pulse",
        "It is children duty to take care of ill parent": "fa-solid fa-hands-holding-child",
        "Jobs scarce": "fa-solid fa-briefcase",
        "Men make better business executives than women do": "fa-solid fa-briefcase-medical",
        "Men make better political leaders than women do": "fa-solid fa-user-tie",
        "Neighbors": "fa-solid fa-people-roof",
        "One of main goals in life has been to make my parents proud": "fa-solid fa-face-smile-beam",
        "People who don't work turn lazy": "fa-solid fa-bed",
        "Pre-school child suffers with working mother": "fa-solid fa-child",
        "Problem if women have more income than husband": "fa-solid fa-scale-unbalanced",
        "University is more important for a boy than for a girl": "fa-solid fa-user-graduate",
        "Work is a duty towards society": "fa-solid fa-briefcase",
        "Work should always come first even if it means less spare time": "fa-solid fa-business-time",
    },
}

def category_icon_for(agrupacion: str, categoria: str) -> str:
    """
    Devuelve el icono Font Awesome para una categoría específica
    dentro de una agrupación. CMP y GPN tienen íconos globales.
    """
    if categoria == "CMP":
        return "fa-solid fa-circle-nodes"
    if categoria == "GPN":
        return "fa-solid fa-globe"

    agr_map = CATEGORY_ICON_MAP.get(agrupacion, {})
    icon = agr_map.get(categoria)
    if icon:
        return icon
    return "fa-solid fa-list-ul"

# ==========================================
# TARJETAS AGRUPACIONES
# ==========================================

def render_card_menu(agrupaciones: List[str]):
    st.markdown("### Selecciona el tema / agrupación que quieres explorar")

    cols = st.columns(3)
    for idx, agr in enumerate(agrupaciones):
        col = cols[idx % 3]
        icon_class = fa_icon_for_group(agr)
        url_agr = quote_plus(agr)
        href = f"?agr={url_agr}"
        card_html = f"""
        <a href="{href}" target="_self" class="wvs-card">
            <div class="wvs-card-icon">
                <i class="{icon_class}"></i>
            </div>
            <div class="wvs-card-text">{agr}</div>
        </a>
        """
        with col:
            st.markdown(card_html, unsafe_allow_html=True)

# ==========================================
# OVERVIEW DEMOGRÁFICO
# ==========================================

def pick_best_respuesta_column(df: pd.DataFrame) -> str:
    """
    Elige la mejor columna de texto para usar como 'respuesta'
    cuando haya varias variantes.
    Orden de preferencia:
      1) respuesta_grafica
      2) respuesta_normalizada
      3) respuesta (original)
    Solo elige columnas que existan en el DataFrame.
    """
    candidates = ["respuesta_grafica", "respuesta_normalizada", COL_RESPUESTA]

    for col in candidates:
        if col in df.columns and df[col].notna().any():
            return col

    # fallback: por si acaso
    return COL_RESPUESTA


DEMOGRAPHIC_KEY_CATEGORIES = ["Age", "Marital status", "Sex"]

def load_demographic_distribution(categoria: str) -> pd.DataFrame:
    query = text(f"""
        SELECT
            {COL_YEAR},
            {COL_RESPUESTA} AS respuesta,
            respuesta_grafica,
            respuesta_normalizada,
            COUNT(*) AS n
        FROM "{PG_SCHEMA}".{PG_TABLE}
        WHERE {COL_AGRUP} = 'Demographic and Socioeconomic'
          AND {COL_CATEGORIA}   = :cat
          AND {COL_RESPUESTA} IS NOT NULL
          AND {COL_YEAR} IN (2020, 2025)
        GROUP BY {COL_YEAR}, {COL_RESPUESTA},
                 respuesta_grafica, respuesta_normalizada
        ORDER BY {COL_YEAR}, n DESC;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cat": categoria})

    if df.empty:
        return df

    # elegir la mejor columna disponible para la respuesta
    best_col = pick_best_respuesta_column(df)

    # usar la columna elegida SOLO para 2025
    df[COL_RESPUESTA] = df.apply(
        lambda r: r[best_col]
        if (r[COL_YEAR] == 2025 and pd.notna(r[best_col]))
        else r[COL_RESPUESTA],
        axis=1
    )

    df[COL_RESPUESTA] = df[COL_RESPUESTA].astype(str).str.strip()

    # calcular porcentajes
    df["total_year"] = df.groupby(COL_YEAR)["n"].transform("sum")
    df["pct"] = (df["n"] / df["total_year"] * 100).round(2)

    # filtro: quitar categorías < 0.5% en 2025
    df = df[~((df[COL_YEAR] == 2025) & (df["pct"] < PCT_MIN_2025))]

    return df



def render_age_plot():
    df = load_demographic_distribution("Age")
    if df.empty:
        st.info("No se encontraron datos para Age.")
        return

    df["edad"] = pd.to_numeric(df["respuesta"], errors="coerce")
    df = df[(df["edad"].notna()) & (df["edad"] >= 0) & (df["edad"] <= 100)]
    if df.empty:
        st.info("No hay datos de edad válidos entre 0 y 100 años.")
        return

    dist = (
        df.groupby([COL_YEAR, "edad"], as_index=False)["n"]
          .sum()
          .sort_values(["edad", COL_YEAR])
    )
    dist["year_str"] = dist[COL_YEAR].astype(str)

    fig = px.line(
        dist,
        x="edad",
        y="n",
        color="year_str",
        markers=True,
        color_discrete_map={"2020": "#1d4ed8", "2025": "#f97316"},
        labels={"edad": "Edad", "n": "Cantidad de respuestas", "year_str": "Año"},
        title="Age – Distribución 2020 vs 2025",
    )
    fig.update_traces(
        hovertemplate="Edad=%{x:.2f}<br>Cantidad=%{y:.2f}<extra></extra>"
    )
    fig.update_layout(
        xaxis=dict(dtick=5, range=[0, 100]),
        yaxis=dict(tickformat=".2f"),
        legend_title_text="Año",
        margin=dict(t=60, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

def render_categorical_plot(categoria: str):
    df = load_demographic_distribution(categoria)
    if df.empty:
        st.info(f"No se encontraron datos para {categoria}.")
        return

    df = df[df["respuesta"].notna()]
    df["respuesta"] = df["respuesta"].astype(str).str.strip()

    order = (
        df.groupby("respuesta")["n"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    df["year_str"] = df[COL_YEAR].astype(str)

    fig = px.bar(
        df,
        x="respuesta",
        y="pct",
        color="year_str",
        barmode="group",
        category_orders={"respuesta": order},
        color_discrete_map={"2020": "#1d4ed8", "2025": "#f97316"},
        labels={"respuesta": "Respuesta", "pct": "% dentro del año", "year_str": "Año"},
        title=f"{categoria} – Distribución 2020 vs 2025",
    )
    fig.update_traces(
        hovertemplate="Respuesta=%{x}<br>% dentro del año=%{y:.2f}<extra></extra>"
    )
    fig.update_yaxes(tickformat=".2f")
    fig.update_xaxes(type="category")  # 👈 forzamos categórico
    fig.update_layout(
        xaxis_tickangle=35,
        bargap=0.25,
        margin=dict(t=60, b=80),
    )
    st.plotly_chart(fig, use_container_width=True)

def render_demographic_overview():
    st.markdown("### Resumen demográfico (2020 vs 2025)")
    tab_age, tab_marital, tab_sex = st.tabs(DEMOGRAPHIC_KEY_CATEGORIES)
    with tab_age:
        render_age_plot()
    with tab_marital:
        render_categorical_plot("Marital status")
    with tab_sex:
        render_categorical_plot("Sex")
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

# ==========================================
# DASHBOARD POR CATEGORÍA (incluye mapas y especificaciones)
# ==========================================

def load_data_for_categoria(agrupacion: str, categoria: str) -> pd.DataFrame:
    q = text(f"""
        SELECT *
        FROM "{PG_SCHEMA}".{PG_TABLE}
        WHERE {COL_AGRUP} = :agr
          AND {COL_CATEGORIA} = :cat
          AND {COL_RESPUESTA} IS NOT NULL
          AND {COL_YEAR} IN (2020, 2025);
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"agr": agrupacion, "cat": categoria})
    return df

def safe_key(base: str) -> str:
    """
    Genera una key "segura" para Streamlit.
    OJO: ya NO recortamos a 80 caracteres para evitar que keys largas
    de distintas especificaciones terminen siendo iguales.
    """
    return base.replace(" ", "_").replace(":", "_").replace("/", "_")


def render_categoria_dashboard(agrupacion: str, categoria: str):
    st.markdown(
        f'<div class="main-title">{categoria}</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        f'<div class="subtitle">Agrupación: {agrupacion}</div>',
        unsafe_allow_html=True
    )

    df = load_data_for_categoria(agrupacion, categoria)
    if df.empty:
        st.info("No hay datos para esta categoría.")
        return

    # Explota multiselect y usa columna “bonita” para la respuesta
    df = explode_multiselect(df, COL_RESPUESTA)
    df = normalize_respuesta_using_label(df)

    total_resp = len(df)
    total_pers = df[COL_RESPONDENT].nunique() if COL_RESPONDENT in df.columns else None

    c1, c2 = st.columns(2)
    c1.metric("Total de respuestas", f"{total_resp:,}")
    c2.metric("Personas únicas", f"{total_pers:,}" if total_pers is not None else "N/D")

    st.markdown("---")

    # ---------------------------------------------------
    # Detectar si esta categoría tiene "especificaciones"
    # ---------------------------------------------------

    def clean_spec(v):
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        low = s.lower()
        if low in ("none", "nan", "sin especificacion", "sin especificación"):
            return None
        return s

    spec_col = None

    if COL_ESPECIF in df.columns:
        df["spec_tmp"] = df[COL_ESPECIF].apply(clean_spec)
        if df["spec_tmp"].notna().any():
            spec_col = "spec_tmp"

    if spec_col is None and COL_LABEL_ES in df.columns:
        df["spec_from_label"] = df[COL_LABEL_ES].apply(
            lambda v: str(v).strip() if pd.notna(v) and str(v).strip() != "" else None
        )
        if df["spec_from_label"].notna().any():
            spec_col = "spec_from_label"

    has_specs = spec_col is not None

    # ==========================================
    # CASO A: NO HAY ESPECIFICACIONES → SOLO GENERAL
    # ==========================================
    if not has_specs:
        st.markdown("### Distribución nacional 2020 vs 2025 (toda la categoría)")

        summary = summarize_by_year(df, COL_RESPUESTA)

        # filtro global: quitar categorías < PCT_MIN_2025 en 2025
        summary = summary[
            ~((summary[COL_YEAR] == 2025) & (summary["pct"] < PCT_MIN_2025))
        ]

        if summary.empty:
            st.info("No hay datos suficientes después del filtro de 0.5% para 2025.")
            return

        summary["Año"] = summary[COL_YEAR].astype(str)

        order_resp = (
            summary.groupby(COL_RESPUESTA)["pct"]
            .mean()
            .sort_values(ascending=False)
            .index.tolist()
        )

        fig = px.bar(
            summary,
            x=COL_RESPUESTA,
            y="pct",
            color="Año",
            barmode="group",
            category_orders={COL_RESPUESTA: order_resp},
            labels={COL_RESPUESTA: "Respuesta", "pct": "% dentro del año"},
            title=f"{categoria} – comparación 2020 vs 2025 (general)",
        )
        fig.update_traces(
            hovertemplate="Respuesta=%{x}<br>% dentro del año=%{y:.2f}<extra></extra>"
        )
        fig.update_xaxes(type="category")
        fig.update_layout(
            yaxis=dict(tickformat=".2f"),
            xaxis_tickangle=-30,
            legend_title_text="Año",
            margin=dict(t=70, b=120),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            summary
            .pivot_table(index=COL_RESPUESTA, columns=COL_YEAR, values="pct", fill_value=0.0)
            .reindex(order_resp)
            .rename(columns={2020: "pct_2020", 2025: "pct_2025"})
            .reset_index()
        )

        # ----- mapa general -----
        st.markdown("---")
        st.markdown("### Mapa por departamento (toda la categoría)")

        if COL_DEPTO not in df.columns or df[COL_DEPTO].dropna().empty:
            st.info("No existe columna de departamento o no hay datos para esta categoría.")
            return

        guate_geo = load_guate_geojson()
        if guate_geo is None:
            st.info("No se encontró el archivo 'mapita.geojson'.")
            return

        años_disp = sorted(df[COL_YEAR].dropna().unique().tolist())
        años_disp = [a for a in años_disp if a in (2020, 2025)]
        if not años_disp:
            st.info("No hay datos 2020/2025 para el mapa.")
            return

        col_sel1, col_sel2 = st.columns(2)
        with col_sel1:
            year_sel = st.selectbox(
                "Año para el mapa (general)",
                años_disp,
                index=0,
                key=safe_key(f"{categoria}_year_general")
            )
        with col_sel2:
            summary_all = summarize_by_year(df, COL_RESPUESTA)
            summary_all = summary_all[
                ~((summary_all[COL_YEAR] == 2025) & (summary_all["pct"] < PCT_MIN_2025))
            ]
            summary_year = summary_all[summary_all[COL_YEAR] == year_sel]

            respuestas_disp = (
                summary_year[COL_RESPUESTA]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )
            respuestas_disp = sorted(respuestas_disp)

            resp_sel = st.selectbox(
                "Respuesta a mapear (% que eligió esta opción, general)",
                respuestas_disp,
                key=safe_key(f"{categoria}_resp_general")
            )

        df_year = df[df[COL_YEAR] == year_sel].copy()
        full_map = build_depto_map_df(df_year, COL_DEPTO, COL_RESPUESTA, resp_sel, guate_geo)

        if full_map.empty:
            st.info("No hay datos para dibujar el mapa general con esta combinación.")
            return

        fig_dep = px.choropleth(
            full_map,
            geojson=guate_geo,
            locations="depto_norm",
            featureidkey="properties.NAME_STD",
            color="pct",
            color_continuous_scale="Blues",
            hover_data={
                "label_depto": True,
                "n": True,
                "total_dep": True,
                "pct": ':.2f',
            },
            labels={
                "pct": "% que eligió la respuesta",
                "label_depto": "Departamento",
                "n": "N respuestas",
                "total_dep": "Total respuestas depto",
            },
            title=f"{year_sel} – '{resp_sel}' (general)",
        )
        fig_dep.update_geos(fitbounds="locations", visible=False)
        fig_dep.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
        st.plotly_chart(fig_dep, use_container_width=True)

        st.markdown("#### Tabla por departamento (general)")
        st.dataframe(
            full_map[["label_depto", "n", "total_dep", "pct"]]
            .sort_values("pct", ascending=False)
            .rename(columns={
                "label_depto": "Departamento",
                "n": "N respuesta",
                "total_dep": "Total depto",
                "pct": "% respuesta",
            })
        )
        return  # fin caso sin especificaciones

    # ==========================================
    # CASO B: SÍ HAY ESPECIFICACIONES → DETALLE
    # ==========================================

    st.markdown("### Detalle por especificación")

    spec_values = sorted(df[spec_col].dropna().unique().tolist())
    if not spec_values:
        st.info("No se encontraron valores de especificación válidos.")
        return

    guate_geo = load_guate_geojson()

    import re  # lo usamos en varias normalizaciones

    for idx_spec, spec in enumerate(spec_values):
        label = str(spec)
        key_suffix = safe_key(f"{idx_spec}_{label}")

        df_spec = df[df[spec_col] == spec]
        if df_spec.empty:
            continue

        # -------------------------------
        # NORMALIZACIONES ESPECIALES
        # -------------------------------
        label_lower = label.lower()

        # 0) CÓDIGO del país: Entrevistado, Madre, Padre → Guatemala / Otras
        if ("código del país" in label_lower) and ("entrevistado, madre, padre" in label_lower):
            def normalize_codigo_pais(v: str) -> str:
                s = str(v).strip().lower()
                if any(x in s for x in ["502", "guatemala", "gtm"]):
                    return "Guatemala"
                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_codigo_pais)

        # 1) Situación actual si eres mujer
        elif "situación actual si eres mujer" in label_lower:
            def normalize_situacion_mujer(v: str) -> str:
                s = str(v).strip().lower()

                if "mujer gestante" in s:
                    return "Mujer gestante"

                if "madre lactante" in s:
                    return "Madre lactante"

                if "mujer no gestante" in s:
                    return "Mujer no gestante"

                if ("niñ" in s) or ("hijo" in s) or ("hija" in s):
                    return "Tiene niño/a"

                if any(x in s for x in ["hombre", "soy hombre", "masculino"]):
                    return "Hombre"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_situacion_mujer)

        # 2) ¿Cuántos hijos cree usted que debieran tener los hogares…?
        elif "cuántos hijos cree usted" in label_lower:
            def normalize_hijos_value(v: str) -> str:
                s = str(v).strip().lower()

                if "2 o 3" in s:
                    return "3"

                nums = re.findall(r"\d+", s)
                if not nums:
                    return "Otras respuestas"

                n = int(nums[0])
                if n > 10:
                    return "Otras respuestas"

                return str(n)

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_hijos_value)

        # 3) ¿A qué edad completó (o completará) su educación a tiempo completo…?
        elif "a qué edad completó (o completará) su educación a tiempo completo" in label_lower:
            def normalize_edad_educ(v: str) -> str:
                s = str(v).strip().lower()

                if "no estudia" in s or "ya no estudia" in s:
                    return "No estudia / no completará"

                if "no indica" in s or "no responde" in s:
                    return "No indica"

                if "no recuerda" in s or "no se" in s or "no sé" in s:
                    return "No sabe / no recuerda"

                if "no aplica" in s:
                    return "No aplica"

                nums = re.findall(r"\d+", s)
                if not nums:
                    return "Otras respuestas"

                edad = int(nums[0])
                if edad < 5 or edad > 80:
                    return "Otras respuestas"

                return str(edad)

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_edad_educ)

        # 4) ¿Cuál es el nivel educativo más alto que usted, su cónyuge, su madre y su padre han alcanzado?
        elif "nivel educativo más alto que usted, su cónyuge, su madre y su padre han alcanzado" in label_lower:
            def normalize_nivel_educ(v: str) -> str:
                s = str(v).strip().lower()

                if s.startswith("0"):
                    return "0 Sin educación / preescolar"
                if s.startswith("1"):
                    return "1 Primaria"
                if s.startswith("2"):
                    return "2 Básica / primer ciclo"
                if s.startswith("3"):
                    return "3 Secundaria / diversificado"

                if any(x in s for x in [
                    "cirug", "ingenier", "medicin", "odontolog", "título de licenciatura",
                    "titulo de licenciatura", "licenciatura", "etc."
                ]):
                    if s.startswith("5"):
                        return "5 Postgrado"
                    return "4 Licenciatura / universitario"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_nivel_educ)

        # 5) ¿Cuánto le toma llegar al centro de salud más cercano?
        elif "cuánto le toma llegar al centro de salud más cercano" in label_lower:
            def normalize_tiempo_salud(v: str) -> str:
                s = str(v).strip().lower()

                if "5 min" in s or "5 minutos" in s:
                    return "5 minutos"
                if "10 min" in s or "10 minutos" in s:
                    return "10 minutos"
                if "15 minutos" in s:
                    return "15 minutos"
                if "20 minutos" in s:
                    return "20 minutos"
                if ("30 minutos" in s) or ("30 min" in s) or ("media hora" in s):
                    return "30 minutos"

                if "1hr" in s or "1 hr" in s or "1 hora" in s:
                    return "1 hora"
                if "2 horas" in s or "dos horas" in s:
                    return "2 horas"
                if "4 horas" in s:
                    return "4 horas"
                if "más de 6 horas" in s or "mas de 6 horas" in s:
                    return "Más de 6 horas"

                if "menos de 1 hora" in s or "minutos" in s or s == "menos":
                    return "Menos de 1 hora"

                if "no indica" in s or "no se" in s or "no sé" in s:
                    return "No indica / no sabe"

                m = re.search(r"(\d+)\s*min", s)
                if m:
                    mins = int(m.group(1))
                    return f"{mins} minutos"

                m = re.search(r"(\d+)\s*hora", s)
                if m:
                    horas = int(m.group(1))
                    if horas == 1:
                        return "1 hora"
                    return f"{horas} horas"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_tiempo_salud)

        # 6) De todo lo que hemos platicado, ¿qué esperaría de un centro de Salud nutricional…?
        elif "centro de salud nutricional para sus hijos" in label_lower:
            def normalize_expectativas_centro(v: str) -> str:
                s = str(v).strip().lower()

                if any(x in s for x in ["ayuda", "apoyo", "apoyar"]):
                    return "Ayuda / apoyo"

                if "atenci" in s or "servicio" in s:
                    return "Buena atención / servicio"

                if "medic" in s:
                    return "Medicamentos"

                if "aliment" in s:
                    return "Plan de alimentación"

                if "médica" in s or "medica" in s or "doctor" in s:
                    return "Atención médica"

                if s in ("si", "sí", "s", ".", ""):
                    return "No especifica / sí"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_expectativas_centro)

        # 7) Si mañana hubiera elecciones, ¿por cuál partido votaría usted…?
        elif "si mañana hubiera elecciones" in label_lower and "por cuál partido votaría" in label_lower:
            def normalize_intencion_voto(v: str) -> str:
                s = str(v).strip().lower()

                # Partidos específicos
                if "semilla" in s:
                    return "Movimiento Semilla"
                if "unidad nacional de la esperanza" in s or " une" in s or s == "une":
                    return "UNE"
                if "valor" in s:
                    return "Valor"
                if "avanzada nacional" in s or " pan" in s or s == "pan":
                    return "PAN"
                if "winaq" in s:
                    return "Winaq"

                # No sabe / ninguno / confidencial
                if "no sabe" in s or "no se" in s or "no sé" in s:
                    return "No sabe"
                if "ninguno" in s or "nadie" in s:
                    return "Ninguno / nadie"
                if "confidencial" in s:
                    return "Confidencial"

                # Todo lo demás
                return "Otros partidos"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_intencion_voto)

        # 8) Si una mujer gana más que su marido es casi seguro que creará problemas
        elif "si una mujer gana más que su marido es casi seguro que creará problemas" in label_lower:
            def normalize_problema_ingreso_mujer(v: str) -> str:
                s = str(v).strip().lower()

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Neutro
                if "ni de acuerdo ni en desacuerdo" in s or "ni acuerdo ni desacuerdo" in s:
                    return "Ni de acuerdo ni en desacuerdo"

                # DESACUERDO 
                if ("totalmente" in s and "desacuerdo" in s) or ("muy" in s and "desacuerdo" in s):
                    return "Totalmente en desacuerdo"
                
                if "desacuerdo" in s:
                    return "En desacuerdo"

                # -----------------------------
                # ACUERDO (UNIFICAR SOLO MUY + TOTALMENTE)
                # -----------------------------
                if ("totalmente" in s and "acuerdo" in s) or ("muy" in s and "acuerdo" in s):
                    return "Totalmente de acuerdo"

                if s == "de acuerdo" or "de acuerdo" in s:
                    return "De acuerdo"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_problema_ingreso_mujer)

        # Jobs scarce – Employers should give priority to (nation) people than immigrants
        elif (
            "employers should give priority" in label_lower
            or "jobs scarce" in label_lower
        ):
            def normalize_jobs_scarce(v: str) -> str:
                s = str(v).strip().lower()

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Neutro
                if "ni de acuerdo ni en desacuerdo" in s or "ni acuerdo ni desacuerdo" in s:
                    return "Ni de acuerdo ni en desacuerdo"

                # DESACUERDO 
                if ("totalmente" in s and "desacuerdo" in s) or ("muy" in s and "desacuerdo" in s):
                    return "Totalmente en desacuerdo"
                
                if "desacuerdo" in s:
                    return "En desacuerdo"

                # -----------------------------
                # ACUERDO (UNIFICAR SOLO MUY + TOTALMENTE)
                # -----------------------------
                if ("totalmente" in s and "acuerdo" in s) or ("muy" in s and "acuerdo" in s):
                    return "Totalmente de acuerdo"

                if s == "de acuerdo" or "de acuerdo" in s:
                    return "De acuerdo"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_jobs_scarce)

        # Men should have more right to a job than women
        elif (
            "men should have more right to a job than women" in label_lower
            or "men should have more right" in label_lower
        ):
            def normalize_men_more_right_job(v: str) -> str:
                s = str(v).strip().lower()

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Neutro
                if "ni de acuerdo ni en desacuerdo" in s or "ni acuerdo ni desacuerdo" in s:
                    return "Ni de acuerdo ni en desacuerdo"

                # -----------------------------
                # DESACUERDO
                # -----------------------------
                # Muy + Totalmente → Totalmente
                if ("totalmente" in s and "desacuerdo" in s) or ("muy" in s and "desacuerdo" in s):
                    return "Totalmente en desacuerdo"

                if "desacuerdo" in s:
                    return "En desacuerdo"

                # -----------------------------
                # ACUERDO (UNIFICAR SOLO MUY + TOTALMENTE)
                # -----------------------------
                if ("totalmente" in s and "acuerdo" in s) or ("muy" in s and "acuerdo" in s):
                    return "Totalmente de acuerdo"

                if s == "de acuerdo" or "de acuerdo" in s:
                    return "De acuerdo"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_men_more_right_job)

        # It is children duty to take care of ill parent
        elif (
            "it is children duty to take care of ill parent" in label_lower
            or "children duty to take care" in label_lower
            or "take care of ill parent" in label_lower
            or "cuidado continuo a sus padres" in label_lower
            or "cuidado continuo" in label_lower
        ):
            def normalize_children_duty_care_parent(v: str) -> str:
                s = str(v).strip().lower()

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Neutro
                if "ni de acuerdo ni en desacuerdo" in s or "ni acuerdo ni desacuerdo" in s:
                    return "Ni de acuerdo ni en desacuerdo"

                # DESACUERDO (unificar muy + totalmente)
                if ("totalmente" in s and "desacuerdo" in s) or ("muy" in s and "desacuerdo" in s):
                    return "Totalmente en desacuerdo"

                if "desacuerdo" in s:
                    return "En desacuerdo"

                # ACUERDO (unificar muy + totalmente)
                if ("totalmente" in s and "acuerdo" in s) or ("muy" in s and "acuerdo" in s):
                    return "Totalmente de acuerdo"

                if s == "de acuerdo" or "de acuerdo" in s:
                    return "De acuerdo"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_children_duty_care_parent)

        elif categoria.strip().lower() == "important in life":
            def normalize_important_in_life(v: str) -> str:
                s = "" if v is None else str(v).strip().lower()
                s = " ".join(s.split())

                # OJO: "no muy importante" debe ir antes que "muy importante"
                if "no muy importante" in s:
                    return "No muy importante"
                if "muy importante" in s:
                    return "Muy importante"
                if "bastante importante" in s:
                    return "Bastante importante"
                if "nada importante" in s:
                    return "Nada importante"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_important_in_life)

         # Important child qualities (multi-select: hasta cinco)
        elif (
            "important child qualities" in label_lower
            or "cualidades que pueden fomentarse en el hogar" in label_lower
            or "cualidades" in label_lower and "hasta cinco" in label_lower
        ):
            def normalize_child_qualities(v: str) -> str:
                s = "" if v is None else str(v).strip().lower()
                s = " ".join(s.split())

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Normalizaciones generales (tildes/variantes típicas)
                s = s.replace("ó", "o").replace("á", "a").replace("é", "e").replace("í", "i").replace("ú", "u")
                s = s.replace(" / ", "/").replace(" /", "/").replace("/ ", "/")
                s = s.replace(" y ", " ").replace("&", " ")
                s = " ".join(s.split())

                # Mapeo a etiqueta estándar (lo que tú quieres ver en gráfica)
                MAP = {
                    "buenos modales": "Buenos modales",
                    "sentido de responsabilidad": "Sentido de responsabilidad",
                    "tolerancia y respeto hacia otros": "Tolerancia y respeto hacia otros",
                    "tolerancia respeto hacia otros": "Tolerancia y respeto hacia otros",
                    "obediencia": "Obediencia",
                    "fe religiosa": "Fe religiosa",
                    "fe religosa": "Fe religiosa",
                    "independencia": "Independencia",
                    "trabajo duro/dedicacion al trabajo": "Trabajo duro / dedicación al trabajo",
                    "trabajo duro dedicacion al trabajo": "Trabajo duro / dedicación al trabajo",
                    "determinacion/perseverancia": "Determinación / perseverancia",
                    "determinacion perseverancia": "Determinación / perseverancia",
                    "generosidad": "Generosidad",
                    "altruismo": "Altruismo",
                    "imaginacion": "Imaginación",
                    "sentido de la economia y espiritu de ahorro": "Sentido de la economía y espíritu de ahorro",
                    "sentido de la economia espiritu de ahorro": "Sentido de la economía y espíritu de ahorro",
                }

                if s in MAP:
                    return MAP[s]

                # fallback: capitaliza “bonito” si no estaba en el mapa
                return str(v).strip()

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_child_qualities)

        # Future changes – Greater respect for authority
        elif (
            "greater respect for authority" in label_lower
            or ("future changes" in label_lower and "respect for authority" in label_lower)
        ):
            def normalize_future_respect_authority(v: str) -> str:
                s = "" if v is None else str(v).strip().lower()
                s = " ".join(s.split())

                # No sabe / no responde
                if any(x in s for x in ["no sabe", "no se", "no responde", "ns/nr", "nsnr"]):
                    return "No sabe / No responde"

                # Normalizar tildes mínimas (por si viene "importaría")
                s = (
                    s.replace("á", "a").replace("é", "e").replace("í", "i")
                     .replace("ó", "o").replace("ú", "u")
                )

                # Categorías
                if s in ("bueno", "bien", "good"):
                    return "Bueno"

                if s in ("malo", "mal", "bad"):
                    return "Malo"

                # "No me importa"
                if ("no me importa" in s) or (s == "no importa") or ("doesnt matter" in s) or ("doesn't matter" in s):
                    return "No me importa"

                # "No le importaría"
                if ("no le importaria" in s) or ("no le importaria" in s) or ("wouldnt mind" in s) or ("wouldn't mind" in s):
                    return "No me importa"

                return "Otras respuestas"

            df_spec = df_spec.copy()
            df_spec[COL_RESPUESTA] = df_spec[COL_RESPUESTA].apply(normalize_future_respect_authority)


        # -------------------------------
        # RESUMEN Y GRÁFICA
        # -------------------------------

        st.markdown(f"#### Especificación: {label}")

        summary_spec = summarize_by_year(df_spec, COL_RESPUESTA)

        # filtro global para especificaciones
        summary_spec = summary_spec[
            ~((summary_spec[COL_YEAR] == 2025) & (summary_spec["pct"] < PCT_MIN_2025))
        ]

        if summary_spec.empty:
            st.info("No hay datos suficientes para esta especificación después del filtro.")
            continue

        summary_spec["Año"] = summary_spec[COL_YEAR].astype(str)

        order_resp_spec = (
            summary_spec.groupby(COL_RESPUESTA)["pct"]
            .mean()
            .sort_values(ascending=False)
            .index.tolist()
        )

        fig_spec = px.bar(
            summary_spec,
            x=COL_RESPUESTA,
            y="pct",
            color="Año",
            barmode="group",
            category_orders={COL_RESPUESTA: order_resp_spec},
            labels={COL_RESPUESTA: "Respuesta", "pct": "% dentro del año"},
            title=f"{categoria} – comparación 2020 vs 2025 ({label})",
        )
        fig_spec.update_traces(
            hovertemplate="Respuesta=%{x}<br>% dentro del año=%{y:.2f}<extra></extra>"
        )
        fig_spec.update_xaxes(type="category")
        fig_spec.update_layout(
            yaxis=dict(tickformat=".2f"),
            xaxis_tickangle=-30,
            legend_title_text="Año",
            margin=dict(t=60, b=100),
        )
        st.plotly_chart(fig_spec, use_container_width=True)

        st.dataframe(
            summary_spec
            .pivot_table(index=COL_RESPUESTA, columns=COL_YEAR, values="pct", fill_value=0.0)
            .reindex(order_resp_spec)
            .rename(columns={2020: "pct_2020", 2025: "pct_2025"})
            .reset_index()
        )

        # ----- mapa por especificación -----
        if COL_DEPTO not in df_spec.columns or df_spec[COL_DEPTO].dropna().empty:
            st.info("No hay datos de departamento para esta especificación.")
            st.markdown("---")
            continue

        if guate_geo is None:
            st.info("No se encontró el archivo 'mapita.geojson'.")
            st.markdown("---")
            continue

        años_disp_spec = sorted(df_spec[COL_YEAR].dropna().unique().tolist())
        años_disp_spec = [a for a in años_disp_spec if a in (2020, 2025)]
        if not años_disp_spec:
            st.info("No hay datos 2020/2025 para esta especificación.")
            st.markdown("---")
            continue

        col_sel1, col_sel2 = st.columns(2)
        with col_sel1:
            year_sel_spec = st.selectbox(
                f"Año para el mapa ({label})",
                años_disp_spec,
                index=0,
                key=safe_key(f"{categoria}_year_{key_suffix}")
            )
        with col_sel2:
            respuestas_disp_spec = (
                summary_spec[summary_spec[COL_YEAR] == year_sel_spec][COL_RESPUESTA]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )
            respuestas_disp_spec = sorted(respuestas_disp_spec)

            if not respuestas_disp_spec:
                st.info("No hay respuestas para mapear en esta especificación.")
                st.markdown("---")
                continue

            resp_sel_spec = st.selectbox(
                f"Respuesta a mapear ({label})",
                respuestas_disp_spec,
                key=safe_key(f"{categoria}_resp_{key_suffix}")
            )

        df_year_spec = df_spec[df_spec[COL_YEAR] == year_sel_spec].copy()
        full_map_spec = build_depto_map_df(
            df_year_spec, COL_DEPTO, COL_RESPUESTA, resp_sel_spec, guate_geo
        )

        if full_map_spec.empty:
            st.info("No hay datos para dibujar el mapa con esta combinación en esta especificación.")
            st.markdown("---")
            continue

        fig_dep_spec = px.choropleth(
            full_map_spec,
            geojson=guate_geo,
            locations="depto_norm",
            featureidkey="properties.NAME_STD",
            color="pct",
            color_continuous_scale="Blues",
            hover_data={
                "label_depto": True,
                "n": True,
                "total_dep": True,
                "pct": ':.2f',
            },
            labels={
                "pct": "% que eligió la respuesta",
                "label_depto": "Departamento",
                "n": "N respuesta",
                "total_dep": "Total respuestas depto",
            },
            title=f"{year_sel_spec} – '{resp_sel_spec}' ({label})",
        )
        fig_dep_spec.update_geos(fitbounds="locations", visible=False)
        fig_dep_spec.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
        st.plotly_chart(fig_dep_spec, use_container_width=True)

        st.markdown(f"##### Tabla por departamento ({label})")
        st.dataframe(
            full_map_spec[["label_depto", "n", "total_dep", "pct"]]
            .sort_values("pct", ascending=False)
            .rename(columns={
                "label_depto": "Departamento",
                "n": "N respuesta",
                "total_dep": "Total depto",
                "pct": "% respuesta",
            })
        )

        st.markdown("---")


# ==========================================
# TARJETAS CATEGORÍAS
# ==========================================

def render_categoria_cards(agrupacion: str, categorias: List[str]):
    st.markdown(f"### Categorías dentro de {agrupacion}")
    cols = st.columns(3)

    for idx, cat in enumerate(categorias):
        if not str(cat).strip():
            continue

        col = cols[idx % 3]

        url_agr = quote_plus(agrupacion)
        url_cat = quote_plus(cat)
        href = f"?agr={url_agr}&cat={url_cat}"

        icon_class = category_icon_for(agrupacion, cat)

        card_html = f"""
        <a href="{href}" target="_self" class="wvs-card-cat">
            <div class="wvs-card-cat-icon">
                <i class="{icon_class}"></i>
            </div>
            <div class="wvs-card-cat-text">{cat}</div>
        </a>
        """

        with col:
            st.markdown(card_html, unsafe_allow_html=True)

# ==========================================
# MAIN
# ==========================================

def main():
    st.markdown('<div class="main-title">World Values Survey – Guatemala</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="subtitle">Exploración interactiva de las respuestas del World Values Survey para Guatemala.</div>',
        unsafe_allow_html=True
    )

    params = st.query_params

    raw_agr = params.get("agr", None)
    if isinstance(raw_agr, list):
        raw_agr = raw_agr[0]

    raw_cat = params.get("cat", None)
    if isinstance(raw_cat, list):
        raw_cat = raw_cat[0]

    selected_agr = unquote_plus(raw_agr) if raw_agr else None
    selected_cat = unquote_plus(raw_cat) if raw_cat else None

    # Vista categoría
    if selected_agr and selected_cat:
        if st.button("⬅ Volver a categorías"):
            if "cat" in st.query_params:
                del st.query_params["cat"]
            st.rerun()

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        render_categoria_dashboard(selected_agr, selected_cat)
        return

    # Vista agrupación
    if selected_agr and not selected_cat:
        if st.button("⬅ Volver a agrupaciones"):
            st.query_params.clear()
            st.rerun()

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown(f"## {selected_agr}")

        if selected_agr == "Demographic and Socioeconomic":
            render_demographic_overview()

        categorias = load_categorias(selected_agr)

        if selected_agr == "Demographic and Socioeconomic":
            categorias = [
                c for c in categorias
                if c not in [
                    "Age",
                    "Country of birth",
                    "Ethnic group",
                    "Marital status",
                    "Sex",
                    "Year of birth",
                ]
            ]

        if categorias:
            render_categoria_cards(selected_agr, categorias)
        else:
            st.info("No hay más categorías para mostrar en esta agrupación.")
        return

    # Vista home
    agrupaciones = load_agrupaciones()
    if not agrupaciones:
        st.error("No se encontraron agrupaciones en la tabla.")
        return

    render_card_menu(agrupaciones)

if __name__ == "__main__":
    main()
