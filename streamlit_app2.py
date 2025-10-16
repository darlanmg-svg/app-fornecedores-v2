# streamlit_app2.py
# App: Fornecedores Próximos à Obra + Consulta CNPJ (com retry/cache/fallback) + Portal da Transparência + PNCP
# Requisitos: streamlit, googlemaps, folium, streamlit-folium, python-dotenv, pandas, requests, urllib3

import os
import math
import time
import logging
from datetime import datetime
import urllib.parse
import json
import glob
import re

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import googlemaps
from folium import Map, Marker, Icon, Popup
from streamlit_folium import st_folium

# -------------------- CONFIG BÁSICA --------------------
st.set_page_config(page_title="Fornecedores Próximos à Obra", layout="wide")
load_dotenv()

# Ambiente e chaves
ENV = os.getenv("ENV") or st.secrets.get("ENV") or "production"
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY") or st.secrets.get("GOOGLE_MAPS_API_KEY")
API_TRANSPARENCIA_KEY = os.getenv("API_TRANSPARENCIA_KEY") or st.secrets.get("API_TRANSPARENCIA_KEY")

# Logging
logging.basicConfig(
    filename="streamlit_buscas.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("fornecedores-obra")

if not API_KEY:
    st.error("Defina GOOGLE_MAPS_API_KEY (Secrets no Streamlit Cloud ou .env local).")
    st.stop()

st.caption(f"Ambiente: **{ENV}**  •  API_TRANSPARENCIA_KEY: **{'SIM' if API_TRANSPARENCIA_KEY else 'NÃO'}**")
gmaps = googlemaps.Client(key=API_KEY)

SEGMENTOS_SUGERIDOS = [
    "concreteira", "madeireira", "locadora de andaimes", "locadora de equipamentos",
    "aço/ferragens", "areia/brita/agregados", "transportadora de entulho", "vidraçaria",
    "drywall/gesso", "hidráulica", "elétrica", "argamassa", "tintas", "telhas", "pré-moldados"
]

# -------------------- ESTADO INICIAL --------------------
if "obra_atual" not in st.session_state:
    st.session_state.obra_atual = None
if "fornecedores_atual" not in st.session_state:
    st.session_state.fornecedores_atual = None
if "df_atual" not in st.session_state:
    st.session_state.df_atual = None
if "cnpj_cache" not in st.session_state:
    st.session_state.cnpj_cache = {}  # {cnpj_digits: dict(json)}
if "last_cnpj_click_ts" not in st.session_state:
    st.session_state.last_cnpj_click_ts = 0.0

# -------------------- UTILS --------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lon1), 
    p1 = math.radians(lat1); p2 = math.radians(lon1)  # fix scoping for readability
    p1 = math.radians(lat1); p2 = math.radians(lon1)
    # re-implement cleanly
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def geocode_endereco(endereco: str):
    ge = gmaps.geocode(endereco)
    if not ge:
        return None
    loc = ge[0]["geometry"]["location"]
    return {"lat": loc["lat"], "lng": loc["lng"], "formatado": ge[0]["formatted_address"]}

def places_nearby(lat, lng, termo, raio_metros=5000, max_results=20, aberto_agora=False):
    """Busca via Nearby; pagina até max_results."""
    results = []
    params = dict(location=(lat, lng), radius=raio_metros, keyword=termo, language="pt-BR", open_now=aberto_agora or None)
    response = gmaps.places_nearby(**{k: v for k, v in params.items() if v is not None})
    while True:
        results.extend(response.get("results", []))
        token = response.get("next_page_token")
        if token and len(results) < max_results:
            time.sleep(2)
            response = gmaps.places_nearby(page_token=token)
        else:
            break
        if len(results) >= max_results:
            break
    return results[:max_results]

def place_details_enriquecido(place_id):
    d = gmaps.place(place_id=place_id, language="pt-BR")
    r = d.get("result", {}) if d else {}
    return {
        "telefone": r.get("formatted_phone_number"),
        "site": r.get("website"),
        "endereco_completo": r.get("formatted_address"),
        "rating": r.get("rating"),
        "user_ratings_total": r.get("user_ratings_total"),
        "open_now": (r.get("opening_hours") or {}).get("open_now"),
        "weekday_text": (r.get("opening_hours") or {}).get("weekday_text"),
        "business_status": r.get("business_status")
    }

def montar_links_busca(nome: str, cidade_estado: str | None = None):
    q = f"{nome} {cidade_estado or ''}".strip()
    return {
        "google_web": "https://www.google.com/search?q=" + urllib.parse.quote_plus(f'{q} construtora obra fornecedor'),
        "reclame_aqui": "https://www.reclameaqui.com.br/busca/?q=" + urllib.parse.quote_plus(q),
        "linkedin_empresas": "https://www.linkedin.com/search/results/companies/?keywords=" + urllib.parse.quote_plus(q),
        "portal_transparencia": "https://www.portaltransparencia.gov.br/busca?termo=" + urllib.parse.quote_plus(q)
    }

def _throttle_ok(min_seconds=2.0):
    now = time.time()
    if now - st.session_state.last_cnpj_click_ts < min_seconds:
        return False, max(0.0, min_seconds - (now - st.session_state.last_cnpj_click_ts))
    st.session_state.last_cnpj_click_ts = now
    return True, 0.0

def _normalize_cnpj(cnpj_raw: str) -> str:
    return "".join([c for c in (cnpj_raw or "") if c.isdigit()])

# -------------------- BRASILAPI (retry/backoff + cache + fallback) --------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def make_session_with_retry(verify_ssl=True):
    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.verify = verify_ssl
    s.headers.update({"User-Agent": "fornecedores-obra/1.0"})
    return s

def _consultar_cnpj_brasilapi_com_retry(cnpj_digits: str):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_digits}"
    s = make_session_with_retry(verify_ssl=False)  # sem validar SSL (teste local)
    t0 = time.time()
    r = s.get(url, timeout=20)
    dt = (time.time() - t0) * 1000
    logger.info(f"BrasilAPI GET {url} -> {r.status_code} ({dt:.0f} ms)")
    if r.status_code == 200:
        return r.json()
    if r.status_code == 429:
        logger.warning("BrasilAPI rate-limited (429)")
        return None  # sinaliza para usar cache/fallback
    try:
        r.raise_for_status()
    except Exception as e:
        logger.error(f"BrasilAPI erro: {e}")
    return None

@st.cache_data(ttl=60*60, show_spinner=False)  # 1h
def consultar_cnpj_brasilapi_cacheado(cnpj_digits: str):
    return _consultar_cnpj_brasilapi_com_retry(cnpj_digits)

def consultar_cnpj_receitaws_best_effort(cnpj_digits: str):
    """Fallback opcional. Sujeito a limites próprios."""
    try:
        url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj_digits}"
        t0 = time.time()
        r = requests.get(url, timeout=25)
        dt = (time.time() - t0) * 1000
        logger.info(f"ReceitaWS GET {url} -> {r.status_code} ({dt:.0f} ms)")
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.warning(f"ReceitaWS falhou: {e}")
    return None

# -------------------- PORTAL DA TRANSPARÊNCIA & PNCP --------------------
PT_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
PT_HEADERS = {"chave-api-dados": API_TRANSPARENCIA_KEY} if API_TRANSPARENCIA_KEY else {}

def _pt_available():
    return bool(API_TRANSPARENCIA_KEY)

def _dbg_count(title, data, meta=None):
    try:
        if isinstance(data, dict) and "content" in data:
            n = len(data.get("content") or [])
        else:
            n = len(data or [])
    except Exception:
        n = 0
    st.caption(f"{title}: itens={n}")

def _badge_origem(api_ok, dump_ok):
    if api_ok:
        st.caption("Fonte: **API (ao vivo)**")
    elif dump_ok:
        st.caption("Fonte: **Arquivo local (fallback)**")
    else:
        st.caption("Fonte: **—**")

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_get_raw(path, params):
    """Chamada crua ao PT (sem paginação), usada pela paginação."""
    if not _pt_available():
        return None, {"erro": "API_TRANSPARENCIA_KEY ausente"}
    try:
        r = requests.get(f"{PT_BASE}/{path}", headers=PT_HEADERS, params=params, timeout=30)
        logger.info(f"PT GET {path} {params} -> {r.status_code}")
        if r.status_code == 200:
            return r.json(), r.headers
        return None, {"status": r.status_code, "text": r.text}
    except Exception as e:
        logger.error(f"PT erro {path}: {e}")
        return None, {"erro": str(e)}

def _pt_fetch_all(path, base_params, page_key_order=("pagina","tamanho"), start_page=1, page_size_default=50, sleep=0.15, max_pages=2000):
    """Pagina até acabar (lista vazia) — retorna lista completa."""
    if not _pt_available():
        return None, {"erro": "API_TRANSPARENCIA_KEY ausente"}
    items = []
    page_key = None
    # detect page key supported
    for k in page_key_order:
        if k in base_params:
            page_key = k
            break
    if not page_key:
        # tenta com 'pagina' se fizer sentido
        page_key = "pagina"
    p = base_params.copy()
    p.setdefault(page_key, start_page)
    p.setdefault("tamanho", base_params.get("tamanho", page_size_default))

    for _ in range(max_pages):
        data, meta = pt_get_raw(path, p)
        if data is None:
            return None, meta
        if isinstance(data, list):
            items.extend(data)
            if len(data) == 0:
                break
            p[page_key] = int(p[page_key]) + 1
            time.sleep(sleep)
        else:
            # objeto único — não paginar
            items.append(data)
            break
    return items, {}

# ---- Endpoints relevantes para análise de fornecedor
@st.cache_data(show_spinner=False, ttl=60*30)
def pt_pessoa_juridica(cnpj_digits):
    return pt_get_raw("pessoa-juridica", {"cnpj": cnpj_digits})

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_contratos_cpf_cnpj(cnpj_digits):
    return _pt_fetch_all("contratos/cpf-cnpj", {"cpfCnpj": cnpj_digits, "tamanho": 100})

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_sancoes(cnpj_digits):
    # CEIS/CNEP/CEPIM unificado em "sancoes"
    return _pt_fetch_all("sancoes", {"documento": cnpj_digits, "tamanho": 100})

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_notas_fiscais(cnpj_digits):
    return _pt_fetch_all("notas-fiscais", {"cnpjEmitente": cnpj_digits, "tamanho": 100})

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_renuncias_valor(cnpj_digits):
    return _pt_fetch_all("renuncias-valor", {"cnpj": cnpj_digits, "tamanho": 100})

@st.cache_data(show_spinner=False, ttl=60*30)
def pt_despesas(cnpj_digits, data_ini, data_fim):
    params = {"cnpjFavorecido": cnpj_digits, "dataInicio": data_ini, "dataFim": data_fim, "tamanho": 100}
    return _pt_fetch_all("despesas", params)

# ---- PNCP (sem chave)
PNCP_BASE = "https://pncp.gov.br/api/pncp"

@st.cache_data(show_spinner=False, ttl=60*30)
def pncp_get(path, params):
    try:
        r = requests.get(f"{PNCP_BASE}/{path}", params=params, timeout=30)
        logger.info(f"PNCP GET {path} {params} -> {r.status_code}")
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        logger.error(f"PNCP erro {path}: {e}")
        return None

@st.cache_data(show_spinner=False, ttl=60*30)
def pncp_avisos_por_cnpj(cnpj_digits, pagina=0, tamanho=50):
    params = {"pagina": pagina, "tamanho": tamanho, "documentoFornecedor": cnpj_digits}
    return pncp_get("v1/avisos", params)

@st.cache_data(show_spinner=False, ttl=60*30)
def pncp_contratos_por_cnpj(cnpj_digits, pagina=0, tamanho=50):
    params = {"pagina": pagina, "tamanho": tamanho, "documentoFornecedor": cnpj_digits}
    return pncp_get("v1/contratos", params)

# -------------------- FALLBACK LOCAL (arquivos do harvester) --------------------
def _paths_local(cnpj_digits):
    # tenta ./saida e /mnt/data
    patterns = [
        f"./saida/*{cnpj_digits}*",
        f"/mnt/data/*{cnpj_digits}*"
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    return files

def _try_read_json_local(cnpj_digits):
    # consolidado do harvester
    for p in [f"./saida/resultado_{cnpj_digits}.json", f"/mnt/data/resultado_{cnpj_digits}.json"]:
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
    return None

def _fallback_from_dump(consolidado, method_path_key):
    # method_path_key, ex.: "GET /api-de-dados/contratos/cpf-cnpj"
    try:
        bloco = consolidado.get(method_path_key)
        return bloco.get("data") if bloco else None
    except Exception:
        return None

def _read_csv_if_exists(name_like, cnpj_digits):
    # tenta pegar CSVs já prontos (contratos, pessoa_juridica, outros)
    candidates = []
    for base in ["./saida", "/mnt/data"]:
        candidates.extend(glob.glob(os.path.join(base, f"{name_like}_{cnpj_digits}.csv")))
    for p in candidates:
        try:
            return pd.read_csv(p)
        except Exception:
            pass
    return None

# -------------------- CACHE PLACES --------------------
@st.cache_data(show_spinner=False, ttl=60*30)
def geocode_endereco_cached(endereco: str):
    return geocode_endereco(endereco)

@st.cache_data(show_spinner=False, ttl=60*30)
def buscar_fornecedores_cached(lat, lng, termo, raio_metros=5000, max_results=60, aberto_agora=False):
    base = places_nearby(lat, lng, termo, raio_metros=raio_metros, max_results=max_results, aberto_agora=aberto_agora)
    detalhes = []
    for r in base:
        loc = r["geometry"]["location"]
        item = {
            "nome": r.get("name"),
            "endereco": r.get("vicinity"),
            "lat": loc["lat"], "lng": loc["lng"],
            "place_id": r.get("place_id"),
        }
        try:
            info = place_details_enriquecido(item["place_id"])
            item.update(info)
        except Exception:
            pass
        detalhes.append(item)
    return detalhes

def desenhar_mapa(obra, fornecedores):
    m = Map(location=[obra["lat"], obra["lng"]], zoom_start=13)
    Marker([obra["lat"], obra["lng"]], tooltip="Obra", icon=Icon(color="red", icon="home", prefix="fa")).add_to(m)
    for f in fornecedores:
        popup_html = f"""
        <b>{f['nome']}</b><br>
        {f.get('endereco_completo') or f.get('endereco') or ''}<br>
        Distância (reta): {f['dist_km']} km<br>
        Nota: {f.get('rating') or '-'} ({f.get('user_ratings_total') or 0} avaliações)<br>
        Tel: {f.get('telefone') or '-'}<br>
        <a href="{f.get('site') or '#'}" target="_blank">{f.get('site') or ''}</a>
        """
        Marker(
            [f["lat"], f["lng"]],
            tooltip=f"{f['nome']} ({f['dist_km']} km)",
            icon=Icon(color="blue", icon="industry", prefix="fa"),
            popup=Popup(popup_html, max_width=320)
        ).add_to(m)
    return m

# -------------------- UI --------------------
st.title("🧭 Fornecedores Próximos à Obra")
st.caption("Foco em proximidade + sinais de reputação. Consulta CNPJ com retry/cache, Portal da Transparência (multi-endpoint) e PNCP.")

tab1, tab3 = st.tabs(["🔎 Busca única", "🧾 Consulta CNPJ"])

# ---------- TAB 1: BUSCA ÚNICA ----------
with tab1:
    with st.form(key="form_busca_unica", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns([4, 3, 2, 3])
        with c1:
            endereco = st.text_input("Endereço da obra", placeholder="Ex: Rua A, Centro, Belo Horizonte - MG")
        with c2:
            seg_sug = st.selectbox("Segmento (sugestões)", options=SEGMENTOS_SUGERIDOS, index=0)
            segmento = st.text_input("Ou personalize o segmento", value=seg_sug)
        with c3:
            raio_km = st.slider("Raio (km)", 1, 30, 5)
        with c4:
            aberto_agora = st.checkbox("Aberto agora", value=False, help="Filtra negócios abertos no momento")
        c5, c6, c7 = st.columns([2, 2, 3])
        with c5:
            nota_min = st.number_input("Nota mínima (0–5)", min_value=0.0, max_value=5.0, value=0.0, step=0.1)
        with c6:
            reviews_min = st.number_input("Mínimo de avaliações", min_value=0, max_value=10000, value=0, step=5)
        with c7:
            priorizar = st.selectbox("Ordenação", ["Distância (reta)", "Nota primeiro", "Mais avaliações primeiro"])

        submitted = st.form_submit_button("Buscar fornecedores", use_container_width=True)

    if submitted:
        if not endereco.strip():
            st.warning("Informe um endereço.")
            st.stop()

        obra = geocode_endereco_cached(endereco)
        if not obra:
            st.error("Endereço não encontrado.")
            st.stop()

        fornecedores = buscar_fornecedores_cached(
            obra["lat"], obra["lng"], segmento,
            raio_metros=int(raio_km * 1000), max_results=60, aberto_agora=aberto_agora
        )

        for f in fornecedores:
            f["dist_km"] = round(haversine_km(obra["lat"], obra["lng"], f["lat"], f["lng"]), 2)

        def passa(f):
            nota = f.get("rating") or 0
            total = f.get("user_ratings_total") or 0
            return (nota >= nota_min) and (total >= reviews_min)

        fornecedores = [f for f in fornecedores if passa(f)]

        if priorizar == "Distância (reta)":
            fornecedores.sort(key=lambda x: x["dist_km"])
        elif priorizar == "Nota primeiro":
            fornecedores.sort(key=lambda x: (-(x.get("rating") or 0), -(x.get("user_ratings_total") or 0), x["dist_km"]))
        else:
            fornecedores.sort(key=lambda x: (-(x.get("user_ratings_total") or 0), -(x.get("rating") or 0), x["dist_km"]))

        st.session_state.obra_atual = obra
        st.session_state.fornecedores_atual = fornecedores

    if st.session_state.fornecedores_atual is not None and st.session_state.obra_atual is not None:
        obra = st.session_state.obra_atual
        fornecedores = st.session_state.fornecedores_atual
        st.success(f"Obra: {obra['formatado']}")

        if len(fornecedores) == 0:
            st.info("Nenhum fornecedor encontrado com os filtros. Amplie o raio ou reduza os mínimos de reputação.")
        else:
            df = pd.DataFrame(fornecedores)[
                ["nome", "endereco_completo", "dist_km", "rating", "user_ratings_total",
                 "telefone", "site", "business_status", "open_now", "weekday_text", "lat", "lng", "place_id"]
            ]
            st.session_state.df_atual = df

            st.dataframe(df.rename(columns={
                "nome": "Nome",
                "endereco_completo": "Endereço",
                "dist_km": "Dist (km, reta)",
                "rating": "Nota",
                "user_ratings_total": "Avaliações",
                "telefone": "Telefone",
                "site": "Site",
                "business_status": "Status",
                "open_now": "Aberto agora",
                "weekday_text": "Horários"
            }), use_container_width=True, hide_index=True)

            st.download_button(
                "Baixar CSV (enriquecido)",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="fornecedores_proximos.csv",
                mime="text/csv",
                use_container_width=True
            )

            mapa = desenhar_mapa(obra, fornecedores)
            st_folium(mapa, use_container_width=True, height=520)

            st.markdown("### 🔗 Investigar fornecedores (links rápidos)")
            for f in fornecedores[:20]:
                cidade_estado_hint = None
                if f.get("endereco_completo"):
                    cidade_estado_hint = ", ".join(f["endereco_completo"].split(",")[-2:]).strip()
                links = montar_links_busca(f["nome"], cidade_estado_hint)
                with st.expander(f"🔍 {f['nome']} — {f.get('endereco_completo','')[:80]}"):
                    st.markdown(
                        f"[Google Web]({links['google_web']})  |  "
                        f"[Reclame Aqui]({links['reclame_aqui']})  |  "
                        f"[LinkedIn (empresas)]({links['linkedin_empresas']})  |  "
                        f"[Portal da Transparência]({links['portal_transparencia']})"
                    )

# ---------- TAB 2: CONSULTA CNPJ ----------
with tab3:
    st.write("Cole um CNPJ para buscar dados cadastrais (BrasilAPI com retry/cache/fallback) e enriquecer com Portal da Transparência e PNCP.")
    cnpj_in = st.text_input("CNPJ (somente números ou formatado)")

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        consultar = st.button("Consultar CNPJ", use_container_width=True)
    with c2:
        testar = st.button("Usar CNPJ de exemplo", use_container_width=True)
    with c3:
        limpar_cache = st.button("Limpar cache", use_container_width=True)

    if limpar_cache:
        st.session_state.cnpj_cache.clear()
        st.success("Cache de CNPJ limpo.")

    if testar:
        cnpj_in = st.selectbox(
            "Escolha um CNPJ de controle",
            ["02558157000162 (Telefônica/Vivo)", "40432544000147 (Claro S.A.)",
             "03317153000107 (SERPRO)", "62318475000120 (Stefanini)"]
        ).split()[0]
        st.info(f"Usando CNPJ de teste: {cnpj_in}")
        consultar = True

    if consultar:
        ok, wait = _throttle_ok(2.0)
        if not ok:
            st.info(f"Segure um pouquinho… aguardando {wait:.1f}s para evitar rate limit.")
            st.stop()

        if not cnpj_in.strip():
            st.warning("Informe um CNPJ.")
        else:
            cnpj_digits = _normalize_cnpj(cnpj_in)
            if len(cnpj_digits) != 14:
                st.error("CNPJ inválido (precisa de 14 dígitos).")
                st.stop()

            logger.info(f"Consulta CNPJ iniciada: {cnpj_digits}")

            # ----------- DADOS CADASTRAIS BÁSICOS (BrasilAPI / fallback) -----------
            origem = "nenhuma"
            data = None

            if cnpj_digits in st.session_state.cnpj_cache:
                data = st.session_state.cnpj_cache[cnpj_digits]
                origem = "cache_sessao"

            if data is None:
                data = consultar_cnpj_brasilapi_cacheado(cnpj_digits)
                if data is not None:
                    origem = "brasilapi"

            if data is None:
                data = consultar_cnpj_receitaws_best_effort(cnpj_digits)
                if data is not None:
                    origem = "fallback_receitaws"

            if data:
                st.session_state.cnpj_cache[cnpj_digits] = data

            st.caption(f"📡 Fonte dos dados cadastrais: **{origem.upper()}**")

            # Render cadastral (tolerante a None)
            if data:
                campos_prefer = {
                    "cnpj": data.get("cnpj") or cnpj_digits,
                    "razao_social": data.get("razao_social") or data.get("nome") or data.get("nome_empresarial"),
                    "nome_fantasia": data.get("nome_fantasia") or data.get("fantasia"),
                    "descricao_porte": data.get("descricao_porte") or data.get("porte"),
                    "cnae_fiscal_descricao": data.get("cnae_fiscal_descricao") or (
                        data.get("atividade_principal", [{"text": None}])[0].get("text")
                        if isinstance(data.get("atividade_principal"), list) else None
                    ),
                    "data_inicio_atividade": data.get("data_inicio_atividade") or data.get("abertura"),
                    "situacao_cadastral": data.get("situacao_cadastral") or data.get("situacao"),
                    "logradouro": data.get("logradouro") or (data.get("estabelecimento") or {}).get("logradouro"),
                    "numero": data.get("numero") or (data.get("estabelecimento") or {}).get("numero"),
                    "bairro": data.get("bairro") or (data.get("estabelecimento") or {}).get("bairro"),
                    "municipio": data.get("municipio") or (data.get("estabelecimento") or {}).get("cidade"),
                    "uf": data.get("uf") or (data.get("estabelecimento") or {}).get("estado"),
                    "cep": data.get("cep") or (data.get("estabelecimento") or {}).get("cep"),
                    "opcao_pelo_simples": data.get("opcao_pelo_simples"),
                    "opcao_pelo_mei": data.get("opcao_pelo_mei"),
                }
                st.json(campos_prefer)
                nome = campos_prefer.get("nome_fantasia") or campos_prefer.get("razao_social") or ""
                cidadeuf = f"{campos_prefer.get('municipio','')}, {campos_prefer.get('uf','')}".strip(", ")
            else:
                st.warning("BrasilAPI/Fallback sem dados no momento. Seguindo apenas com bases públicas (PT/PNCP).")
                nome, cidadeuf = "", ""

            links = montar_links_busca(nome, cidadeuf)
            st.markdown(
                f"[Google Web]({links['google_web']})  |  "
                f"[Reclame Aqui]({links['reclame_aqui']})  |  "
                f"[LinkedIn (empresas)]({links['linkedin_empresas']})  |  "
                f"[Portal da Transparência]({links['portal_transparencia']})"
            )

            # ---------- JANELA TEMPORAL (DESPESAS) ----------
            data_fim = datetime.today().strftime("%Y-%m-%d")
            try:
                mes = pd.DateOffset(months=24)
                data_ini = (pd.Timestamp.today().normalize().replace(day=1) - mes).strftime("%Y-%m-%d")
            except Exception:
                data_ini = datetime.today().strftime("%Y-%m-01")

            # ---------- FALLBACK CONSOLIDADO ----------
            dump = _try_read_json_local(cnpj_digits)

            # ---------- PESSOA JURÍDICA ----------
            with st.expander("🧾 Perfil – Pessoa Jurídica (Portal da Transparência)"):
                api_ok = False; dump_ok = False
                pj, meta_pj = (None, {})
                if _pt_available():
                    pj, meta_pj = pt_pessoa_juridica(cnpj_digits)
                    api_ok = bool(pj)
                if not pj and dump:
                    pj = _fallback_from_dump(dump, "GET /api-de-dados/pessoa-juridica")
                    dump_ok = bool(pj)

                if pj:
                    df_pj = pd.DataFrame(pj if isinstance(pj, list) else [pj])
                    prefer = [c for c in [
                        "cnpj","razaoSocial","nomeFantasia","favorecidoDespesas","possuiContratacao",
                        "participanteLicitacao","emitiuNFe","beneficiadoRenunciaFiscal"
                    ] if c in df_pj.columns]
                    st.dataframe(df_pj[prefer] if prefer else df_pj, use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Pessoa Jurídica",
                        data=(df_pj[prefer] if prefer else df_pj).to_csv(index=False).encode("utf-8"),
                        file_name=f"pt_pessoa_juridica_{cnpj_digits}.csv", mime="text/csv",
                        use_container_width=True)
                else:
                    # CSV específico, se existir
                    df_pj_csv = _read_csv_if_exists("pessoa_juridica", cnpj_digits)
                    if df_pj_csv is not None:
                        st.dataframe(df_pj_csv, use_container_width=True, hide_index=True)
                        dump_ok = True
                    else:
                        st.info("Sem dados de pessoa jurídica para este CNPJ.")
                _badge_origem(api_ok, dump_ok)

            # ---------- SANÇÕES ----------
            with st.expander("⚖️ Sanções (CEIS/CNEP/CEPIM) – Portal da Transparência"):
                api_ok = False; dump_ok = False
                sanc, meta = (None, {})
                if _pt_available():
                    sanc, meta = pt_sancoes(cnpj_digits)
                    api_ok = bool(sanc)
                if not sanc and dump:
                    sanc = _fallback_from_dump(dump, "GET /api-de-dados/sancoes")
                    dump_ok = bool(sanc)

                if sanc:
                    df_s = pd.DataFrame(sanc)
                    if "pessoa" in df_s.columns:
                        df_s["Nome"] = df_s["pessoa"].apply(lambda x: (x or {}).get("nome"))
                    if "orgao" in df_s.columns:
                        df_s["Órgão"] = df_s["orgao"].apply(lambda x: (x or {}).get("nome"))
                    if "tipoSancao" in df_s.columns: df_s["Sanção"] = df_s["tipoSancao"]
                    if "dataPublicacao" in df_s.columns: df_s["Publicação"] = df_s["dataPublicacao"]
                    if "dataFinal" in df_s.columns: df_s["Vigência (fim)"] = df_s["dataFinal"]
                    show = [c for c in ["Nome","Sanção","Órgão","Publicação","Vigência (fim)"] if c in df_s.columns]
                    st.dataframe(df_s[show] if show else df_s, use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Sanções",
                        data=df_s.to_csv(index=False).encode("utf-8"),
                        file_name=f"sancoes_{cnpj_digits}.csv", mime="text/csv", use_container_width=True)
                else:
                    st.info("Nenhuma sanção encontrada.")
                _badge_origem(api_ok, dump_ok)

            # ---------- NOTAS FISCAIS ----------
            with st.expander("🧾 Notas Fiscais emitidas ao Governo – Portal da Transparência"):
                api_ok = False; dump_ok = False
                notas, meta = (None, {})
                if _pt_available():
                    notas, meta = pt_notas_fiscais(cnpj_digits)
                    api_ok = bool(notas)
                if not notas and dump:
                    notas = _fallback_from_dump(dump, "GET /api-de-dados/notas-fiscais")
                    dump_ok = bool(notas)

                if notas:
                    df_nf = pd.DataFrame(notas)
                    prefer = [c for c in [
                        "dataEmissao","numero","serie","valorNotaFiscal","orgaoSuperiorDestinatario",
                        "orgaoDestinatario","chaveNotaFiscal"
                    ] if c in df_nf.columns]
                    st.dataframe(df_nf[prefer] if prefer else df_nf, use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Notas Fiscais",
                        data=(df_nf[prefer] if prefer else df_nf).to_csv(index=False).encode("utf-8"),
                        file_name=f"pt_notas_fiscais_{cnpj_digits}.csv", mime="text/csv",
                        use_container_width=True)
                else:
                    st.info("Nenhuma NFe encontrada para o CNPJ.")
                _badge_origem(api_ok, dump_ok)

            # ---------- RENÚNCIAS ----------
            with st.expander("🏷️ Renúncias de Receita (benefícios fiscais) – Portal da Transparência"):
                api_ok = False; dump_ok = False
                ren, meta = (None, {})
                if _pt_available():
                    ren, meta = pt_renuncias_valor(cnpj_digits)
                    api_ok = bool(ren)
                if not ren and dump:
                    ren = _fallback_from_dump(dump, "GET /api-de-dados/renuncias-valor")
                    dump_ok = bool(ren)

                if ren:
                    df_r = pd.DataFrame(ren)
                    prefer = [c for c in [
                        "ano","tributo","descricaoBeneficioFiscal","valorRenunciado",
                        "formaTributacao","descricaoFundamentoLegal"
                    ] if c in df_r.columns]
                    st.dataframe(df_r[prefer] if prefer else df_r, use_container_width=True, hide_index=True)
                    try:
                        total_r = pd.to_numeric(df_r.get("valorRenunciado", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        st.metric("Total renunciado (somatório)", f"R$ {total_r:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
                    except Exception:
                        pass
                    st.download_button("Baixar CSV – Renúncias",
                        data=(df_r[prefer] if prefer else df_r).to_csv(index=False).encode("utf-8"),
                        file_name=f"pt_renuncias_{cnpj_digits}.csv", mime="text/csv",
                        use_container_width=True)
                else:
                    st.info("Nenhuma renúncia encontrada para o CNPJ.")
                _badge_origem(api_ok, dump_ok)

            # ---------- DESPESAS ----------
            with st.expander("💸 Despesas pagas ao CNPJ – Portal da Transparência (últimos 24 meses)"):
                api_ok = False; dump_ok = False
                dep, meta = (None, {})
                if _pt_available():
                    dep, meta = pt_despesas(cnpj_digits, data_ini, data_fim)
                    api_ok = bool(dep)
                if not dep and dump:
                    # o consolidado do harvester pode não ter a janela; então tenta CSV 'outros'
                    df_dep_csv = _read_csv_if_exists("outros", cnpj_digits)
                    if df_dep_csv is not None:
                        st.dataframe(df_dep_csv, use_container_width=True, hide_index=True)
                        try:
                            total_dep = pd.to_numeric(df_dep_csv.get("valorPagamento", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                            st.metric("Total pago (CSV)", f"R$ {total_dep:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
                        except Exception:
                            pass
                        dump_ok = True
                        dep = None  # já exibimos CSV
                if dep:
                    df_d = pd.DataFrame(dep)
                    prefer = [c for c in [
                        "data","fase","orgaoSuperior","orgao","favorecido","valorPagamento",
                        "empenho","funcao","subfuncao","programa","acao"
                    ] if c in df_d.columns]
                    st.dataframe(df_d[prefer] if prefer else df_d, use_container_width=True, hide_index=True)
                    try:
                        total = pd.to_numeric(df_d.get("valorPagamento", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        st.metric("Total pago (período)", f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
                    except Exception:
                        pass
                    st.download_button("Baixar CSV – Despesas",
                        data=(df_d[prefer] if prefer else df_d).to_csv(index=False).encode("utf-8"),
                        file_name=f"despesas_{cnpj_digits}.csv", mime="text/csv", use_container_width=True)
                elif dep is None and not dump_ok:
                    st.info("Sem pagamentos no período consultado.")
                _badge_origem(api_ok, dump_ok)

            # ---------- CONTRATOS (PT) ----------
            with st.expander("📄 Contratos com o Governo Federal – Portal da Transparência"):
                api_ok = False; dump_ok = False
                ctr, meta = (None, {})
                if _pt_available():
                    ctr, meta = pt_contratos_cpf_cnpj(cnpj_digits)
                    api_ok = bool(ctr)
                if not ctr and dump:
                    ctr = _fallback_from_dump(dump, "GET /api-de-dados/contratos/cpf-cnpj")
                    dump_ok = bool(ctr)

                if ctr:
                    df_c = pd.DataFrame(ctr)
                    # enriquecimentos rápidos
                    if "unidadeGestora" in df_c.columns and "Órgão" not in df_c.columns:
                        try:
                            df_c["Órgão"] = df_c["unidadeGestora"].apply(lambda x: (x or {}).get("nome"))
                        except Exception:
                            pass
                    prefer = [c for c in [
                        "numero","objeto","dataAssinatura","dataInicioVigencia","dataFimVigencia",
                        "valorInicialCompra","valorFinalCompra","Órgão"
                    ] if c in df_c.columns]
                    st.dataframe(df_c[prefer] if prefer else df_c, use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Contratos (PT)",
                        data=(df_c[prefer] if prefer else df_c).to_csv(index=False).encode("utf-8"),
                        file_name=f"contratos_pt_{cnpj_digits}.csv", mime="text/csv",
                        use_container_width=True)
                else:
                    # CSV pronto (do harvester)
                    df_ctr_csv = _read_csv_if_exists("contratos", cnpj_digits)
                    if df_ctr_csv is not None:
                        st.dataframe(df_ctr_csv, use_container_width=True, hide_index=True)
                        dump_ok = True
                    else:
                        st.info("Nenhum contrato encontrado.")
                _badge_origem(api_ok, dump_ok)

            # ---------- PNCP ----------
            with st.expander("🏛️ PNCP – Avisos de Licitação e Contratos"):
                avisos = pncp_avisos_por_cnpj(cnpj_digits)
                contratos_pncp = pncp_contratos_por_cnpj(cnpj_digits)
                nothing = True

                if avisos is not None: _dbg_count("PNCP Avisos", avisos)
                if contratos_pncp is not None: _dbg_count("PNCP Contratos", contratos_pncp)

                if avisos:
                    df_a = pd.DataFrame(avisos.get("content") or avisos)
                    st.markdown("**Avisos de Licitação**")
                    st.dataframe(df_a.head(200), use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Avisos PNCP",
                        data=df_a.to_csv(index=False).encode("utf-8"),
                        file_name=f"pncp_avisos_{cnpj_digits}.csv", mime="text/csv", use_container_width=True)
                    nothing = False

                if contratos_pncp:
                    df_pc = pd.DataFrame(contratos_pncp.get("content") or contratos_pncp)
                    st.markdown("**Contratos PNCP**")
                    st.dataframe(df_pc.head(200), use_container_width=True, hide_index=True)
                    st.download_button("Baixar CSV – Contratos PNCP",
                        data=df_pc.to_csv(index=False).encode("utf-8"),
                        file_name=f"pncp_contratos_{cnpj_digits}.csv", mime="text/csv", use_container_width=True)
                    nothing = False

                if nothing:
                    st.info("Nada encontrado no PNCP para este CNPJ.")

            # --- Dicas/boas práticas
            st.markdown("---")
            st.subheader("Sugestões")
            st.markdown(
                "- Use um **CNPJ de exemplo** para validar as integrações quando receber vazios.\n"
                "- O app aplica **throttle de 2s** entre consultas para evitar *rate limit*.\n"
                "- O fluxo usa **cache + retry/backoff** e **fallback** (ReceitaWS/arquivos locais) quando necessário.\n"
                "- Para **QSA/capital social/CNAEs secundários** sem rate limit, considere baixar o **dump oficial do CNPJ** e consultar localmente."
            )
