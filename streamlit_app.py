# streamlit_app.py
# App: Fornecedores Próximos à Obra (Design v3.0 - Cadastro de Obras com Google Sheets)

import os
import math
import time
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import googlemaps
from folium import Map, Marker, Icon, Popup
from streamlit_folium import st_folium
import gspread

# --- CONFIG BÁSICA (sem alterações) ---
st.set_page_config(page_title="Busca de Fornecedores", layout="wide")
load_dotenv()
st.markdown("""
    <style>
    [data-testid="stForm"] small { display: none; }
    </style>
""", unsafe_allow_html=True)
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    st.error("Defina GOOGLE_MAPS_API_KEY no seu .env ou Secrets do Streamlit.")
    st.stop()
gmaps = googlemaps.Client(key=API_KEY)

# --- ALTERAÇÃO 1: CONEXÃO COM GOOGLE SHEETS ---

# Cacheia a conexão para não reabrir a cada interação
@st.cache_resource
def connect_to_gsheet():
    try:
        # Tenta conectar usando os Secrets do Streamlit
        creds = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds)
        # Coloque o NOME EXATO da sua planilha aqui
        sh = sa.open("BD_Fornecedores_App")
        return sh
    except Exception as e:
        st.error(f"Erro ao conectar com o Google Sheets: {e}")
        st.info("Verifique se o arquivo `secrets.toml` está configurado corretamente e se a planilha foi compartilhada.")
        return None

# Cacheia os dados lidos para performance
@st.cache_data(ttl=600) # Cache de 10 minutos
def load_obras_from_sheet(_sheet_connection):
    if _sheet_connection is None:
        return {}
    try:
        worksheet = _sheet_connection.worksheet("obras") # Nome da aba
        rows = worksheet.get_all_records()
        # Transforma a lista de dicionários no formato que já usamos: {"nome": "endereco"}
        obras_dict = {row["nome_obra"]: row["endereco"] for row in rows if row.get("nome_obra")}
        return obras_dict
    except gspread.exceptions.WorksheetNotFound:
        st.error("Aba 'obras' não encontrada na planilha. Verifique o nome.")
        return {}
    except Exception as e:
        st.error(f"Erro ao ler os dados da planilha: {e}")
        return {}

# --- LÓGICA PRINCIPAL ---
sheet_connection = connect_to_gsheet()
OBRAS = load_obras_from_sheet(sheet_connection)

if not OBRAS:
    st.warning("Nenhuma obra foi carregada da planilha. A lista de seleção estará vazia.")

# O restante do seu script (funções haversine_km, geocode_endereco, etc.) continua aqui...
# Vou omitir por brevidade, pois elas não mudam.
# ... (COLE AS FUNÇÕES ANTERIORES AQUI) ...

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def geocode_endereco(endereco: str):
    ge = gmaps.geocode(endereco)
    if not ge: return None
    loc = ge[0]["geometry"]["location"]
    return {"lat": loc["lat"], "lng": loc["lng"], "formatado": ge[0]["formatted_address"]}

def buscar_fornecedores(lat, lng, termo, raio_metros=5000, max_results=20):
    results = []
    response = gmaps.places_nearby(location=(lat, lng), radius=raio_metros, keyword=termo, language="pt-BR")
    while True:
        for r in response.get("results", []):
            loc = r["geometry"]["location"]
            results.append({"nome": r.get("name"), "endereco": r.get("vicinity"), "lat": loc["lat"], "lng": loc["lng"], "place_id": r.get("place_id"), "avaliacao": r.get("rating")})
        token = response.get("next_page_token")
        if token and len(results) < max_results:
            time.sleep(2)
            response = gmaps.places_nearby(page_token=token)
        else: break
        if len(results) >= max_results: break
    detalhes = []
    for r in results[:max_results]:
        try:
            d = gmaps.place(place_id=r["place_id"], language="pt-BR")
            info = d.get("result", {})
            r["telefone"] = info.get("formatted_phone_number")
            r["site"] = info.get("website")
            r["endereco_completo"] = info.get("formatted_address") or r.get("endereco")
        except Exception:
            r["telefone"], r["site"], r["endereco_completo"] = None, None, r.get("endereco")
        r["dist_km"] = round(haversine_km(lat, lng, r["lat"], r["lng"]), 2)
        detalhes.append(r)
    detalhes.sort(key=lambda x: x["dist_km"])
    return detalhes

def desenhar_mapa(obra, fornecedores):
    m = Map(location=[obra["lat"], obra["lng"]], zoom_start=13)
    Marker([obra["lat"], obra["lng"]], tooltip="Ponto de Referência", icon=Icon(color="red", icon="home", prefix="fa")).add_to(m)
    for f in fornecedores:
        popup_html = f"<b>{f['nome']}</b><br>{f.get('endereco_completo') or ''}<br>Distância: {f['dist_km']} km<br>Tel: {f.get('telefone') or '-'}<br><a href='{f.get('site') or '#'}' target='_blank'>{f.get('site') or ''}</a>"
        Marker([f["lat"], f["lng"]], tooltip=f"{f['nome']} ({f['dist_km']} km)", icon=Icon(color="blue", icon="industry", prefix="fa"), popup=Popup(popup_html, max_width=300)).add_to(m)
    return m

@st.cache_data(show_spinner=False, ttl=60*30)
def geocode_endereco_cached(endereco: str):
    return geocode_endereco(endereco)

@st.cache_data(show_spinner=False, ttl=60*30)
def buscar_fornecedores_cached(lat, lng, termo, raio_metros=5000, max_results=30):
    return buscar_fornecedores(lat, lng, termo, raio_metros=raio_metros, max_results=max_results)

# --- UI - BARRA LATERAL (SIDEBAR) ---
with st.sidebar:
    st.image("logo.png", width=150)
    st.title("Filtros da Busca")
    st.divider()

    # --- ALTERAÇÃO 2: Formulário de cadastro de nova obra ---
    with st.expander("🔗 Cadastrar Nova Obra"):
        with st.form("form_nova_obra"):
            novo_nome = st.text_input("Nome da Nova Obra")
            novo_endereco = st.text_input("Endereço Completo da Nova Obra")
            submitted_nova_obra = st.form_submit_button("Salvar Nova Obra")

            if submitted_nova_obra:
                if novo_nome and novo_endereco:
                    if sheet_connection:
                        try:
                            worksheet = sheet_connection.worksheet("obras")
                            # Verifica se o nome da obra já existe para evitar duplicatas
                            if novo_nome in OBRAS:
                                st.warning(f"A obra '{novo_nome}' já existe.")
                            else:
                                worksheet.append_row([novo_nome, novo_endereco])
                                st.success(f"Obra '{novo_nome}' cadastrada com sucesso!")
                                # Limpa o cache para forçar a releitura dos dados da planilha
                                st.cache_data.clear()
                                st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar na planilha: {e}")
                else:
                    st.error("Preencha o nome e o endereço da nova obra.")
    
    st.divider()
    # O restante da sidebar continua o mesmo, mas agora `OBRAS` vem da planilha
    # ... (COLE A LÓGICA DA SIDEBAR ANTERIOR AQUI) ...
    OPCAO_NOVO_ENDERECO = "Digitar novo endereço..."
    opcoes_obras = [OPCAO_NOVO_ENDERECO] + list(OBRAS.keys())
    obra_selecionada = st.selectbox(
        "1. Selecione a obra ou digite um endereço",
        options=opcoes_obras,
        index=0
    )

    SEGMENTOS_SUGERIDOS = ["concreteira", "madeireira", "locadora de andaimes", "locadora de equipamentos", "aço/ferragens", "areia/brita/agregados", "transportadora de entulho", "vidraçaria", "drywall/gesso", "hidráulica", "elétrica", "argamassa", "tintas", "telhas", "pré-moldados"]
    OPCAO_NOVO_SEGMENTO = "Digitar segmento personalizado..."
    opcoes_segmentos = [OPCAO_NOVO_SEGMENTO] + SEGMENTOS_SUGERIDOS
    segmento_selecionado = st.selectbox(
        "2. Escolha um segmento sugerido",
        options=opcoes_segmentos,
        index=0
    )
    
    st.divider()
    
    with st.form(key="form_busca"):
        if obra_selecionada == OPCAO_NOVO_ENDERECO:
            endereco_input = st.text_input("Endereço final para a busca", value="", placeholder="Ex: Av. Paulista, 1000, São Paulo")
        else:
            endereco_input = st.text_input("Endereço final para a busca", value=OBRAS.get(obra_selecionada, ""))
        
        if segmento_selecionado == OPCAO_NOVO_SEGMENTO:
            segmento_input = st.text_input("Segmento final para a busca", value="", placeholder="Ex: parafusos")
        else:
            segmento_input = st.text_input("Segmento final para a busca", value=segmento_selecionado)
        
        st.write("---")

        raio_km = st.slider("3. Raio de busca (km)", 1, 50, 10)

        submitted = st.form_submit_button("🔍 Buscar Fornecedores", use_container_width=True)

# ... (COLE A LÓGICA DE PROCESSAMENTO E A UI PRINCIPAL AQUI) ...
# O resto do código permanece igual. Ele vai usar a variável `OBRAS` que foi carregada dinamicamente.
if 'fornecedores_atual' not in st.session_state:
    st.session_state.fornecedores_atual = None

if submitted:
    endereco_final = endereco_input.strip()
    segmento_final = segmento_input.strip()

    if not endereco_final:
        st.sidebar.error("❌ O endereço não pode estar vazio.")
    if not segmento_final:
        st.sidebar.error("❌ O segmento não pode estar vazio.")
    
    if not endereco_final or not segmento_final:
        st.stop()

    if obra_selecionada == OPCAO_NOVO_ENDERECO:
        nome_da_busca = endereco_final
    else:
        nome_da_busca = obra_selecionada

    with st.spinner(f"Buscando '{segmento_final}' perto de '{nome_da_busca}'..."):
        obra = geocode_endereco_cached(endereco_final)
        if not obra:
            st.error(f"Endereço não foi encontrado: '{endereco_final}'. Por favor, tente ser mais específico.")
            st.stop()
        
        fornecedores = buscar_fornecedores_cached(
            obra["lat"], obra["lng"], segmento_final,
            raio_metros=int(raio_km * 1000), max_results=40
        )
        st.session_state.obra_atual = obra
        st.session_state.fornecedores_atual = fornecedores
        st.session_state.raio_atual = raio_km
        st.session_state.segmento_atual = segmento_final
        st.session_state.nome_da_busca = nome_da_busca

st.title("🗺️ Mapa de Fornecedores")
st.caption("Resultados da busca de fornecedores próximos ao local selecionado")
st.divider()

if st.session_state.fornecedores_atual is None:
    st.info("⬅️ Utilize os filtros na barra lateral para iniciar uma busca.")
    st.page_link("https://www.google.com/maps", label="Abrir Google Maps", icon="🌍")
else:
    obra = st.session_state.obra_atual
    fornecedores = st.session_state.fornecedores_atual
    raio_km = st.session_state.raio_atual
    segmento = st.session_state.segmento_atual
    nome_da_busca = st.session_state.nome_da_busca
    
    col1, col2 = st.columns(2)
    col1.metric("Fornecedores Encontrados", f"{len(fornecedores)}", help=f"Busca por '{segmento}'")
    col2.metric("Raio da Busca", f"{raio_km} km")

    st.success(f"**Ponto de Referência:** {obra['formatado']}")
    
    if not fornecedores:
        st.warning("Nenhum fornecedor encontrado com os critérios definidos. Tente ampliar o raio ou alterar o segmento.")
    else:
        with st.expander("Ver/Ocultar Tabela de Resultados", expanded=True):
            df = pd.DataFrame(fornecedores)[["nome", "dist_km", "endereco_completo", "telefone", "site", "avaliacao"]]
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                "Baixar CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"fornecedores_{segmento.replace(' ', '_')}_{nome_da_busca.replace(' ', '_').replace('/', '_')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        st.divider()
        
        st.subheader("Localização no Mapa")
        mapa = desenhar_mapa(obra, fornecedores)
        st_folium(mapa, use_container_width=True, height=500)