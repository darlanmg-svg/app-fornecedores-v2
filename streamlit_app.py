# streamlit_app.py
# App: Fornecedores Próximos à Obra
# Requisitos: streamlit, googlemaps, folium, streamlit-folium, python-dotenv, pandas

import os
import math
import time
import logging
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import googlemaps
from folium import Map, Marker, Icon, Popup
from streamlit_folium import st_folium

# -------------------- CONFIG BÁSICA --------------------
st.set_page_config(page_title="Fornecedores Próximos à Obra", layout="wide")
load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    st.error("Defina GOOGLE_MAPS_API_KEY no seu .env ou Secrets do Streamlit.")
    st.stop()

gmaps = googlemaps.Client(key=API_KEY)

SEGMENTOS_SUGERIDOS = [
    "concreteira", "madeireira", "locadora de andaimes", "locadora de equipamentos",
    "aço/ferragens", "areia/brita/agregados", "transportadora de entulho", "vidraçaria",
    "drywall/gesso", "hidráulica", "elétrica", "argamassa", "tintas", "telhas", "pré-moldados"
]

# Logger simples para arquivo local
logging.basicConfig(
    filename="streamlit_buscas.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# -------------------- ESTADO INICIAL --------------------
if "obra_atual" not in st.session_state:
    st.session_state.obra_atual = None
if "fornecedores_atual" not in st.session_state:
    st.session_state.fornecedores_atual = None
if "df_atual" not in st.session_state:
    st.session_state.df_atual = None

# -------------------- FUNÇÕES ÚTEIS --------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def geocode_endereco(endereco: str):
    ge = gmaps.geocode(endereco)
    if not ge:
        return None
    loc = ge[0]["geometry"]["location"]
    return {"lat": loc["lat"], "lng": loc["lng"], "formatado": ge[0]["formatted_address"]}

def buscar_fornecedores(lat, lng, termo, raio_metros=5000, max_results=20):
    # Busca básica por Places Nearby (keyword + radius)
    results = []
    response = gmaps.places_nearby(
        location=(lat, lng),
        radius=raio_metros,
        keyword=termo,
        language="pt-BR"
    )
    while True:
        for r in response.get("results", []):
            name = r.get("name")
            vic = r.get("vicinity")
            loc = r["geometry"]["location"]
            place_id = r.get("place_id")
            rating = r.get("rating")
            results.append({
                "nome": name,
                "endereco": vic,
                "lat": loc["lat"],
                "lng": loc["lng"],
                "place_id": place_id,
                "avaliacao": rating
            })
        token = response.get("next_page_token")
        if token and len(results) < max_results:
            time.sleep(2)  # exigência da API antes de usar o token
            response = gmaps.places_nearby(page_token=token)
        else:
            break
        if len(results) >= max_results:
            break

    # Enriquecimento com detalhes (telefone, site, endereço completo)
    detalhes = []
    for r in results[:max_results]:
        try:
            d = gmaps.place(place_id=r["place_id"], language="pt-BR")
            info = d.get("result", {})
            r["telefone"] = info.get("formatted_phone_number")
            r["site"] = info.get("website")
            r["endereco_completo"] = info.get("formatted_address") or r.get("endereco")
        except Exception:
            r["telefone"] = None
            r["site"] = None
            r["endereco_completo"] = r.get("endereco")
        r["dist_km"] = round(haversine_km(lat, lng, r["lat"], r["lng"]), 2)
        detalhes.append(r)

    detalhes.sort(key=lambda x: x["dist_km"])
    return detalhes

def desenhar_mapa(obra, fornecedores):
    m = Map(location=[obra["lat"], obra["lng"]], zoom_start=13)
    # Pino da obra
    Marker(
        [obra["lat"], obra["lng"]],
        tooltip="Obra",
        icon=Icon(color="red", icon="home", prefix="fa")
    ).add_to(m)

    for f in fornecedores:
        popup_html = f"""
        <b>{f['nome']}</b><br>
        {f.get('endereco_completo') or ''}<br>
        Distância: {f['dist_km']} km<br>
        Tel: {f.get('telefone') or '-'}<br>
        <a href="{f.get('site') or '#'}" target="_blank">{f.get('site') or ''}</a>
        """
        Marker(
            [f["lat"], f["lng"]],
            tooltip=f"{f['nome']} ({f['dist_km']} km)",
            icon=Icon(color="blue", icon="industry", prefix="fa"),
            popup=Popup(popup_html, max_width=300)
        ).add_to(m)
    return m

# -------------------- CACHE NAS CHAMADAS CARAS --------------------
@st.cache_data(show_spinner=False, ttl=60*30)  # 30 min
def geocode_endereco_cached(endereco: str):
    return geocode_endereco(endereco)

@st.cache_data(show_spinner=False, ttl=60*30)
def buscar_fornecedores_cached(lat, lng, termo, raio_metros=5000, max_results=30):
    return buscar_fornecedores(lat, lng, termo, raio_metros=raio_metros, max_results=max_results)

# -------------------- UI --------------------
st.title("🧭 Fornecedores Próximos à Obra")
st.caption("Busque fornecedores (ex.: concreteiras) próximos a um endereço. Suporta busca única e processamento em lote.")

tab1, tab2 = st.tabs(["🔎 Busca única", "📦 Lote (N obras × N segmentos)"])

# ---------- TAB 1: BUSCA ÚNICA ----------
with tab1:
    with st.form(key="form_busca_unica", clear_on_submit=False):
        col1, col2, col3 = st.columns([4, 3, 2])
        with col1:
            endereco = st.text_input("Endereço da obra", placeholder="Ex: Rua A, Centro, Belo Horizonte - MG")
        with col2:
            segmento_sel = st.selectbox("Segmento (sugestões)", options=SEGMENTOS_SUGERIDOS, index=0,
                                        help="Escolha um sugerido e/ou personalize abaixo.")
            segmento = st.text_input("Ou personalize o segmento", value=segmento_sel)
        with col3:
            raio_km = st.slider("Raio (km)", 1, 30, 5)

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
            raio_metros=int(raio_km * 1000), max_results=30
        )

        # Persistir no estado
        st.session_state.obra_atual = obra
        st.session_state.fornecedores_atual = fornecedores

        # Logging CSV + arquivo
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_row = pd.DataFrame([{
                "timestamp": ts,
                "obra_endereco_input": endereco,
                "obra_geocode": obra["formatado"],
                "segmento": segmento,
                "raio_km": raio_km,
                "qtd_resultados": len(fornecedores)
            }])
            if os.path.exists("buscas_log.csv"):
                log_row.to_csv("buscas_log.csv", mode="a", header=False, index=False, encoding="utf-8")
            else:
                log_row.to_csv("buscas_log.csv", index=False, encoding="utf-8")
            logging.info(f"Busca OK | {segmento} | {obra['formatado']} | {len(fornecedores)} resultados")
        except Exception as e:
            logging.exception(f"Falha ao logar busca: {e}")

    # Render estável (mesmo após rerun)
    if st.session_state.fornecedores_atual is not None and st.session_state.obra_atual is not None:
        obra = st.session_state.obra_atual
        fornecedores = st.session_state.fornecedores_atual

        st.success(f"Obra: {obra['formatado']}")
        if len(fornecedores) == 0:
            st.info("Nenhum fornecedor encontrado nesse raio/termo. Tente ampliar o raio ou ajustar o segmento.")
        else:
            df = pd.DataFrame(fornecedores)[
                ["nome", "endereco_completo", "dist_km", "telefone", "site", "avaliacao", "lat", "lng", "place_id"]
            ]
            st.session_state.df_atual = df
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                "Baixar CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="fornecedores_proximos.csv",
                mime="text/csv",
                use_container_width=True
            )
            mapa = desenhar_mapa(obra, fornecedores)
            st_folium(mapa, use_container_width=True, height=520)

# ---------- TAB 2: LOTE ----------
with tab2:
    st.write("Envie uma planilha CSV com **N obras × N segmentos** para processamento em lote.")
    st.markdown("""
**Formato do CSV**  
Colunas obrigatórias:
- `obra_endereco` – endereço completo da obra  
- `segmento` – ex: concreteira, madeireira…

Opcional:
- `raio_km` – se ausente, usa o seletor abaixo
""")
    raio_lote = st.slider("Raio padrão (km) para linhas sem `raio_km`", 1, 30, 8)
    file = st.file_uploader("Carregar CSV", type=["csv"])

    if file is not None:
        try:
            lote = pd.read_csv(file)
        except Exception as e:
            st.error(f"Não foi possível ler o CSV: {e}")
            lote = None

        if lote is not None:
            exigidas = {"obra_endereco", "segmento"}
            if not exigidas.issubset(set(lote.columns)):
                st.error(f"CSV deve conter colunas: {', '.join(sorted(exigidas))}")
            else:
                resultados = []
                with st.spinner("Processando lote…"):
                    for i, row in lote.iterrows():
                        end = str(row["obra_endereco"])
                        seg = str(row["segmento"])
                        rk = float(row["raio_km"]) if "raio_km" in lote.columns and not pd.isna(row["raio_km"]) else raio_lote
                        obra = geocode_endereco_cached(end)
                        if not obra:
                            resultados.append({
                                "obra_endereco": end, "segmento": seg, "erro": "Endereço não encontrado"
                            })
                            continue
                        fornecedores = buscar_fornecedores_cached(
                            obra["lat"], obra["lng"], seg,
                            raio_metros=int(rk * 1000), max_results=30
                        )
                        for f in fornecedores:
                            resultados.append({
                                "obra_endereco": obra["formatado"],
                                "segmento": seg,
                                "nome": f["nome"],
                                "endereco_completo": f.get("endereco_completo") or f.get("endereco"),
                                "dist_km": f["dist_km"],
                                "telefone": f.get("telefone"),
                                "site": f.get("site"),
                                "avaliacao": f.get("avaliacao"),
                                "lat": f["lat"], "lng": f["lng"], "place_id": f.get("place_id")
                            })
                df_res = pd.DataFrame(resultados)
                st.dataframe(df_res, use_container_width=True, hide_index=True)
                st.download_button(
                    "Baixar CSV consolidado",
                    data=df_res.to_csv(index=False).encode("utf-8"),
                    file_name="fornecedores_lote.csv",
                    mime="text/csv",
                    use_container_width=True
                )
