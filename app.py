import os
import io
import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from huggingface_hub import hf_hub_download

# -------------------------
# Config & basic
# -------------------------
st.set_page_config(page_title="Consulta CNPJ (Receita Federal)", layout="wide")
load_dotenv()  # só precisa se o dataset for privado

REPO_ID = "fabiofachini/receitafederal"
PARQUET_PATH_IN_REPO = "data/dados_estabelecimentos_brasil.parquet"
HF_TOKEN = os.getenv("HF_TOKEN", None)

# -------------------------
# Header com LinkedIn (topo esquerdo)
# -------------------------
colL, colR = st.columns([1, 5])
st.title("📊 Consulta de Estabelecimentos (CNPJ)")

st.caption("Fonte: Receita Federal do Brasil • Dataset publicado no Hugging Face • Consulta acelerada com DuckDB")

# -------------------------
# Cache helpers
# -------------------------
@st.cache_resource(show_spinner=False)
def get_local_parquet_path():
    return hf_hub_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        filename=PARQUET_PATH_IN_REPO,
        token=HF_TOKEN,
        local_dir=None
    )

@st.cache_resource(show_spinner=False)
def get_conn():
    return duckdb.connect(database=':memory:')

# -------------------------
# Build WHERE
# -------------------------
def build_where_and_params(
    texto_busca, estados, porte, setor, classe, matriz_filial, municipio_contains,
    cnae_principal_contains, capital_min, capital_max, email_somente_preenchido
):
    where = []
    params = {}

    if texto_busca:
        where.append("(cnpj ILIKE $tb OR razao_social ILIKE $tb OR nome_fantasia ILIKE $tb)")
        params["tb"] = f"%{texto_busca}%"

    if estados:
        placeholders = []
        for i, uf in enumerate(estados):
            key = f"uf{i}"
            params[key] = uf
            placeholders.append(f"${key}")
        where.append(f"estado IN ({', '.join(placeholders)})")

    if porte:
        placeholders = []
        for i, p in enumerate(porte):
            key = f"po{i}"
            params[key] = p
            placeholders.append(f"${key}")
        where.append(f"porte IN ({', '.join(placeholders)})")

    if setor:
        placeholders = []
        for i, s in enumerate(setor):
            key = f"se{i}"
            params[key] = s
            placeholders.append(f"${key}")
        where.append(f"setor IN ({', '.join(placeholders)})")

    if classe:
        placeholders = []
        for i, c in enumerate(classe):
            key = f"cl{i}"
            params[key] = c
            placeholders.append(f"${key}")
        where.append(f"classe IN ({', '.join(placeholders)})")

    if matriz_filial and matriz_filial != "Todos":
        where.append("matriz_filial = $mf")
        params["mf"] = matriz_filial

    if municipio_contains:
        where.append("municipio ILIKE $mun")
        params["mun"] = f"%{municipio_contains}%"

    if cnae_principal_contains:
        where.append("cnae_principal ILIKE $cnae")
        params["cnae"] = f"%{cnae_principal_contains}%"

    if capital_min is not None:
        where.append("capital_num >= $capmin")
        params["capmin"] = float(capital_min)
    if capital_max is not None:
        where.append("capital_num <= $capmax")
        params["capmax"] = float(capital_max)

    if email_somente_preenchido:
        where.append("(email IS NOT NULL AND email <> 'null' AND length(trim(email)) > 0)")

    if where:
        return "WHERE " + " AND ".join(where), params
    return "", params

# -------------------------
# Prepare data: download, view, facets
# -------------------------
parquet_local = get_local_parquet_path()
con = get_conn()

# VIEW com normalizações
con.execute(f"""
    CREATE OR REPLACE VIEW v_empresas AS
    SELECT
        cnpj,
        razao_social,
        nome_fantasia,
        matriz_filial,
        porte,
        COALESCE(
            TRY_CAST(capital_social AS DOUBLE),
            TRY_CAST(REPLACE(CAST(capital_social AS VARCHAR), '"', '') AS DOUBLE)
        ) AS capital_num,
        capital_social,
        estado,
        municipio,
        bairro,
        cep,
        logradouro_tipo,
        logradouro,
        numero,
        endereco,
        CAST(cod_cnae AS VARCHAR) AS cod_cnae,
        cnae_principal,
        setor,
        classe,
        telefone_1,
        telefone_2,
        NULLIF(email, 'null') AS email
    FROM read_parquet('{parquet_local}');
""")

@st.cache_data(show_spinner=False)
def load_facets():
    def flat_unique(q):
        vals = [r[0] for r in con.execute(q).fetchall()]
        # remove None e duplicadas, mantém ordem
        seen, out = set(), []
        for v in vals:
            if v is None: 
                continue
            if v not in seen:
                seen.add(v); out.append(v)
        return out

    ufs = flat_unique("SELECT DISTINCT estado FROM v_empresas ORDER BY 1;")
    portes = flat_unique("SELECT DISTINCT porte FROM v_empresas ORDER BY 1;")
    setores = flat_unique("SELECT DISTINCT setor FROM v_empresas ORDER BY 1;")
    classes = flat_unique("SELECT DISTINCT classe FROM v_empresas ORDER BY 1;")
    return ufs, portes, setores, classes

ufs, portes, setores, classes = load_facets()

# -------------------------
# Sidebar (render UMA VEZ, com listas prontas)
# -------------------------
with st.sidebar:
# ====== Sidebar Header Profissional ======
    AVATAR_URL = "https://avatars.githubusercontent.com/u/000000?v=4"  # Coloque um avatar aqui
    NOME = "Fabio Fachini"
    SUB = "Engenheiro de Dados e IA"
    LINKEDIN = "https://www.linkedin.com/in/fabio-fachini/"
    GITHUB = "https://github.com/fabiofachini"

    st.markdown("""
    <style>
    .sidebar-card * { box-sizing: border-box; }

    /* Cores automáticas para light/dark */
    :root {
    --card-bg: rgba(255,255,255,0.7);
    --text-strong: #111;
    --text-muted: #6b7280;
    --border: #e5e7eb;
    --chip: #f3f4f6;
    }
    @media (prefers-color-scheme: dark) {
    :root {
        --card-bg: rgba(17,24,39,0.6);
        --text-strong: #f7fafc;
        --text-muted: #9aa4b2;
        --border: #1f2937;
        --chip: #111827;
    }
    }

    /* Cartão */
    .sidebar-card{
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 14px;
    margin-bottom: 14px;
    box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }

    .dev-head{
    display:flex; align-items:center; gap:12px;
    }
    .dev-avatar{
    width:48px; height:48px; border-radius:50%;
    object-fit:cover; border:1px solid var(--border);
    }
    .dev-name{
    margin:0; font-weight:700; font-size:1.05rem; color:var(--text-strong);
    }
    .dev-sub{
    margin:2px 0 0; font-size:.85rem; color:var(--text-muted);
    }

    .dev-actions{
    display:flex; gap:8px; margin-top:10px;
    }
    .dev-link{
    display:inline-flex; align-items:center; gap:6px;
    padding:6px 10px; border-radius:999px; text-decoration:none;
    background:var(--chip); color:var(--text-strong); font-weight:600; font-size:.85rem;
    border:1px solid var(--border);
    transition:0.2s ease;
    }
    .dev-link:hover{ filter:brightness(1.05); }

    .dev-icon{ width:16px; height:16px; display:inline-block; }
    </style>

    <div class="sidebar-card">
    <div class="dev-head">
        <img class="dev-avatar" src='""" + AVATAR_URL + """' onerror="this.style.display='none';">
        <div>
        <p class="dev-name">""" + NOME + """</p>
        <p class="dev-sub">""" + SUB + """</p>
        </div>
    </div>

    <div class="dev-actions">
        <a class="dev-link" href='""" + LINKEDIN + """' target="_blank" rel="noopener">
        <svg class="dev-icon" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
            <path d="M19 3A2.94 2.94 0 0 1 22 6v12a2.94 2.94 0 0 1-3 3H5a2.94 2.94 0 0 1-3-3V6a2.94 2.94 0 0 1 3-3h14zM8.34 18v-8H5.67v8h2.67zM7 8.57a1.55 1.55 0 1 0 0-3.1 1.55 1.55 0 0 0 0 3.1zM18.33 18v-4.29c0-2.29-1.22-3.35-2.84-3.35a2.44 2.44 0 0 0-2.2 1.21h-.03V10H10.6v8h2.67v-4.2c0-1.11.2-2.19 1.59-2.19 1.37 0 1.39 1.28 1.39 2.26V18h2.08z"/>
        </svg>
        LinkedIn
        </a>
        <a class="dev-link" href='""" + GITHUB + """' target="_blank" rel="noopener">
        <svg class="dev-icon" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
            <path d="M12 0C5.37 0 0 5.48 0 12.24c0 5.4 3.44 9.97 8.21 11.59.6.11.82-.26.82-.58 0-.29-.01-1.06-.02-2.09-3.34.74-4.04-1.63-4.04-1.63-.55-1.43-1.35-1.81-1.35-1.81-1.1-.77.08-.76.08-.76 1.21.09 1.84 1.26 1.84 1.26 1.08 1.89 2.83 1.34 3.52 1.03.11-.8.42-1.34.76-1.65-2.67-.31-5.48-1.38-5.48-6.15 0-1.36.46-2.47 1.23-3.34-.12-.31-.54-1.55.12-3.23 0 0 1.01-.33 3.3 1.26a11.18 11.18 0 0 1 3.01-.41c1.02 0 2.05.14 3.01.41 2.29-1.59 3.3-1.26 3.3-1.26.66 1.68.24 2.92.12 3.23.77.87 1.23 1.98 1.23 3.34 0 4.79-2.81 5.83-5.49 6.14.43.37.82 1.1.82 2.22 0 1.6-.01 2.89-.01 3.28 0 .32.22.7.83.58 4.76-1.62 8.19-6.19 8.19-11.59C24 5.48 18.63 0 12 0z"/>
        </svg>
        GitHub
        </a>
    </div>
    </div>
    """, unsafe_allow_html=True)


    # ====== /Sidebar Header Profissional ======


    # CSS para deixar o botão vermelho
    st.markdown("""
        <style>
        div.stButton > button:first-child {
            background-color: #ff4b4b;
            color: white;
            border: none;
            padding: 0.6em;
            font-weight: bold;
        }
        div.stButton > button:first-child:hover {
            background-color: #ff3333;
            color: white;
        }
        </style>
    """, unsafe_allow_html=True)

    # Botão vermelho no topo
    run = st.button("Aplicar filtros", use_container_width=True)

    st.header("Filtros")

    texto_busca = st.text_input("Buscar (CNPJ / Razão / Fantasia)", "")

    estados = st.multiselect("Estado (UF)", options=ufs, default=[], placeholder="Selecione UFs")
    porte = st.multiselect("Porte", options=portes, default=[], placeholder="Selecione portes")
    setor = st.multiselect("Setor", options=setores, default=[], placeholder="Selecione setores")
    classe = st.multiselect("Classe CNAE (macro)", options=classes, default=[], placeholder="Selecione classes")

    matriz_filial = st.selectbox("Tipo", ["Todos", "matriz", "filial"], index=0)
    municipio_contains = st.text_input("Município contém", "")
    cnae_principal_contains = st.text_input("CNAE principal contém", "")

    col_a, col_b = st.columns(2)
    with col_a:
        capital_min = st.number_input("Capital social mín.", min_value=0.0, step=1000.0, value=0.0, format="%.2f")
    with col_b:
        capital_max = st.number_input("Capital social máx.", min_value=0.0, step=1000.0, value=0.0, format="%.2f")

    email_somente_preenchido = st.checkbox("Somente com e-mail preenchido", value=False)

    limit = st.number_input("Máx. linhas para exibição", min_value=100, max_value=100000, value=1000, step=100)

# -------------------------
# Query execution (on click)
# -------------------------
if run:
    cap_min = capital_min if capital_min and capital_min > 0 else None
    cap_max = capital_max if capital_max and capital_max > 0 else None

    where_sql, params = build_where_and_params(
        texto_busca, estados, porte, setor, classe, matriz_filial,
        municipio_contains, cnae_principal_contains, cap_min, cap_max,
        email_somente_preenchido
    )

    sql = f"""
        WITH base AS (
            SELECT *
            FROM v_empresas
            {where_sql}
        )
        SELECT * FROM base
        LIMIT {int(limit)};
    """

    df = con.execute(sql, params).df()
    cnt = con.execute(f"SELECT COUNT(*) FROM v_empresas {where_sql};", params).fetchone()[0]

    st.subheader(f"Resultado: {cnt:,} linha(s) (mostrando até {limit:,})")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads
    col1, col2 = st.columns(2)
    with col1:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar CSV (resultado mostrado)",
            data=csv_bytes,
            file_name="consulta_estabelecimentos.csv",
            mime="text/csv",
            use_container_width=True
        )
    with col2:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        st.download_button(
            "⬇️ Baixar Parquet (resultado mostrado)",
            data=buf.getvalue(),
            file_name="consulta_estabelecimentos.parquet",
            mime="application/octet-stream",
            use_container_width=True
        )
else:
    st.info("Ajuste os filtros na barra lateral e clique em **Aplicar filtros** para consultar.")
