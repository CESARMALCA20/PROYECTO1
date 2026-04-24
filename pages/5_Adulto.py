import streamlit as st
import polars as pl
import pandas as pd
import os

# ─── CONFIGURACIÓN DE PÁGINA ──────────────────────────────────────────────────
pd.set_option("styler.render.max_elements", 1000000)
st.set_page_config(
    layout="wide",
    page_title="Auditoría Adulto - Red San Pablo",
    page_icon="🏥",
    initial_sidebar_state="expanded"
)

# ─── CSS PROFESIONAL (TEMA CLARO) ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp {
    background: #f0f6ff;
    background-image:
        radial-gradient(ellipse at 10% 20%, rgba(14,165,233,0.07) 0%, transparent 50%),
        radial-gradient(ellipse at 90% 80%, rgba(6,182,212,0.05) 0%, transparent 50%);
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #ffffff 0%, #f0f6ff 100%);
    border-right: 1px solid rgba(14,165,233,0.2);
}
[data-testid="stSidebar"] * { color: #1e293b !important; }
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stTextInput label {
    color: #475569 !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase;
}
[data-testid="stSidebar"] [data-baseweb="select"] {
    background: #ffffff !important;
    border: 1px solid rgba(14,165,233,0.4) !important;
    border-radius: 8px !important;
}

[data-testid="stSidebarNavItems"],
[data-testid="stSidebarNav"],
section[data-testid="stSidebarNav"],
div[data-testid="stSidebarNavItems"] { display: none !important; }

.btn-back {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: linear-gradient(135deg, #e0f2fe, #f0f9ff);
    border: 1px solid rgba(14,165,233,0.5);
    color: #0369a1 !important;
    padding: 10px 20px;
    border-radius: 10px;
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-decoration: none !important;
    cursor: pointer;
    transition: all 0.3s ease;
    width: 100%;
    text-align: center;
    justify-content: center;
    margin-bottom: 20px;
    box-sizing: border-box;
}
.btn-back:hover {
    background: linear-gradient(135deg, #bae6fd, #e0f2fe);
    border-color: #0ea5e9;
    box-shadow: 0 4px 16px rgba(14,165,233,0.25);
    transform: translateY(-1px);
}

.hero-header { text-align: center; padding: 30px 20px 10px; margin-bottom: 10px; }
.hero-tag {
    display: inline-block;
    background: #e0f2fe;
    border: 1px solid rgba(14,165,233,0.4);
    color: #0369a1;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 4px 14px;
    border-radius: 20px;
    margin-bottom: 12px;
}
.hero-title {
    font-family: 'Rajdhani', sans-serif;
    font-size: 2.4rem;
    font-weight: 700;
    color: #0f172a;
    letter-spacing: 0.04em;
    line-height: 1.1;
    margin: 0;
}
.hero-title span {
    background: linear-gradient(90deg, #0369a1, #0ea5e9, #06b6d4);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.hero-sub { font-size: 0.85rem; color: #64748b; margin-top: 8px; letter-spacing: 0.05em; }
.divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(14,165,233,0.4), transparent);
    margin: 16px 0 24px;
}

.metric-card {
    background: #ffffff;
    border: 1px solid rgba(14,165,233,0.25);
    border-radius: 14px;
    padding: 22px 24px;
    text-align: center;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
    box-shadow: 0 2px 8px rgba(14,165,233,0.08);
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, #0ea5e9, #06b6d4);
}
.metric-card:hover {
    border-color: rgba(14,165,233,0.5);
    box-shadow: 0 8px 28px rgba(14,165,233,0.15);
    transform: translateY(-2px);
}
.metric-icon { font-size: 1.6rem; margin-bottom: 8px; }
.metric-label {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #94a3b8;
    margin-bottom: 6px;
}
.metric-value {
    font-family: 'Rajdhani', sans-serif;
    font-size: 2.2rem;
    font-weight: 700;
    color: #0369a1;
    line-height: 1;
}
.metric-value.green  { color: #059669; }
.metric-value.yellow { color: #d97706; }
.metric-value.red    { color: #dc2626; }

.stDataFrame { border: 1px solid rgba(14,165,233,0.15) !important; border-radius: 12px !important; overflow: hidden !important; }

/* ── FILA HOVER Y SELECCIÓN ── */
[data-testid="stDataFrame"] tr:hover td {
    background-color: rgba(14,165,233,0.08) !important;
    transition: background-color 0.15s ease;
}
[data-testid="stDataFrame"] tr.row-selected td,
[data-testid="stDataFrame"] tr[aria-selected="true"] td {
    background-color: rgba(14,165,233,0.15) !important;
    box-shadow: inset 0 0 0 1px rgba(14,165,233,0.3);
}
/* Glint en la celda activa */
[data-testid="stDataFrame"] td:focus,
[data-testid="stDataFrame"] td[aria-selected="true"] {
    outline: 2px solid rgba(14,165,233,0.5) !important;
    outline-offset: -2px;
    background-color: rgba(14,165,233,0.12) !important;
}

.stDownloadButton button {
    background: linear-gradient(135deg, #0ea5e9, #06b6d4) !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    letter-spacing: 0.05em !important;
    padding: 10px 24px !important;
    font-size: 0.85rem !important;
    box-shadow: 0 4px 16px rgba(14,165,233,0.3) !important;
}

.filtros-header {
    font-family: 'Rajdhani', sans-serif;
    font-size: 1.1rem;
    font-weight: 700;
    color: #0369a1;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 12px 0 8px;
    border-bottom: 1px solid rgba(14,165,233,0.25);
    margin-bottom: 16px;
}
.sidebar-section {
    background: #f0f9ff;
    border: 1px solid rgba(14,165,233,0.2);
    border-radius: 10px;
    padding: 12px;
    margin-bottom: 14px;
}

.no-data { text-align: center; padding: 60px 20px; color: #94a3b8; }
.no-data-icon { font-size: 3rem; margin-bottom: 12px; }
.no-data-title { font-family: 'Rajdhani', sans-serif; font-size: 1.4rem; font-weight: 600; color: #64748b; }
</style>
""", unsafe_allow_html=True)


# ─── CARGA DE DATOS ───────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)

ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

@st.cache_data
def cargar_datos():
    if not os.path.exists(ARCHIVO_PARQUET):
        return None
    try:
        df = pl.read_parquet(ARCHIVO_PARQUET)
        df = df.rename({col: col.strip() for col in df.columns})
        return df
    except Exception as e:
        st.error(f"Error técnico al leer Parquet: {e}")
        return None

df_raw = cargar_datos()
if df_raw is None:
    st.error(f"⚠️ Archivo no encontrado: `{ARCHIVO_PARQUET}`")
    st.stop()

# ─── ITEMS CONFIG ─────────────────────────────────────────────────────────────
# (Codigo_Item en el parquet, ID_columna_pivot único, Etiqueta visual)
# 99801_TA  → Codigo_Item="99801" + Valor_Lab="TA"
# 99801_1   → Codigo_Item="99801" + Valor_Lab="1"
# resto     → solo coincidencia exacta de Codigo_Item (cualquier Valor_Lab)
ITEMS_CONFIG = [
    ("99801",    "99801_TA",   "99801\nPLAN ELABORADO"),
    ("99401",    "99401",      "99401\nCONSEJERIA INT."),
    ("Z019",     "Z019",       "Z019\nVALORACIÓN RIESGO"),
    ("Z017",     "Z017",       "Z017\nEXAM. LABORATORIO"),
    ("99209.02", "99209.02",   "99209.02\nIMC"),
    ("99209.03", "99209.03",   "99209.03\nPERIMETRO ABD."),
    ("99199.22", "99199.22",   "99199.22\nPRESION ART."),
    ("99401.13", "99401.13",   "99401.13\nESTILOS DE VIDA"),
    ("96150.01", "96150.01",   "96150.01\nTAMIZAJE VIF M/F"),
    ("96150.02", "96150.02",   "96150.02\nALCOHOL Y DROGAS"),
    ("96150.03", "96150.03",   "96150.03\nDEPRESIÓN PHQ-9"),
    ("99402.09", "99402.09",   "99402.09\nCONS. S. MENTAL"),
    ("99173",    "99173",      "99173\nAGUDEZA VISUAL"),
    ("99401.16", "99401.16",   "99401.16\nCONS. S. OCULAR"),
    ("99401.33", "99401.33",   "99401.33\nPRE-TEST VIH"),
    ("86703.01", "86703.01",   "86703.01\nDETECCION VIH"),
    ("86318.01", "86318.01",   "86318.01\nVIH/SIFILIS"),
    ("99401.34", "99401.34",   "99401.34\nPOST-TEST VIH"),
    ("D0150",    "D0150",      "D0150\nORAL"),
    ("99801",    "99801_1",    "99801\nPLAN EJECUTADO"),
    ("99402.03", "99402.03",   "99402.03\nCONS. SSYRR"),
    ("90688",    "90688",      "90688\nVAC. INF. CUADRI."),
    ("90658",    "90658",      "90658\nVAC. INF. TRIVAL."),
    ("Z030",     "Z030",       "Z030\nTAMIZAJE TB"),
    ("99199.58", "99199.58",   "99199.58\nTBC"),
    ("87342",    "87342",      "87342\nHEPATITIS B"),
    ("88141.01", "88141.01",   "88141.01\nCANCER UTERO"),
    ("84152",    "84152",      "84152\nCANCER PROSTATA"),
    ("82270",    "82270",      "82270\nCANCER COLON"),
    ("Z128",     "Z128",       "Z128\nCANCER PIEL"),
]
N_ITEMS = len(ITEMS_CONFIG)

# ─── MESES ESPAÑOL ───────────────────────────────────────────────────────────
MESES_ES = {
    "January": "ENERO",    "February": "FEBRERO",  "March": "MARZO",
    "April": "ABRIL",      "May": "MAYO",           "June": "JUNIO",
    "July": "JULIO",       "August": "AGOSTO",      "September": "SETIEMBRE",
    "October": "OCTUBRE",  "November": "NOVIEMBRE", "December": "DICIEMBRE"
}

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <a href="/" target="_self" class="btn-back">← Regresar al Menú Principal</a>
    """, unsafe_allow_html=True)

    st.markdown('<div class="filtros-header">⚙ Filtros</div>', unsafe_allow_html=True)

    df_tmp = df_raw.with_columns([
        pl.col("Fecha_Atencion").cast(pl.Date),
        pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"),
        pl.col("Fecha_Atencion").dt.strftime("%B").alias("Mes_Nombre"),
    ]).with_columns(pl.col("Mes_Nombre").replace(MESES_ES))

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    lista_ipress = sorted([str(i).strip() for i in df_tmp["Nombre_Establecimiento"].unique().to_list()])
    target_name  = "SAN LUIS BAJO - GRANDE"
    default_sel  = [i for i in lista_ipress if target_name in i]
    sel_ipress   = st.multiselect("🏥 IPRESS", options=lista_ipress, default=default_sel)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    df_meses_lista = df_tmp.select(["Mes_Num", "Mes_Nombre"]).unique().sort("Mes_Num")
    sel_mes = st.multiselect("📅 Mes de Atención", options=df_meses_lista["Mes_Nombre"].to_list())
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    sel_dni = st.text_input("🔍 Buscar por DNI")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(
        '<p style="font-size:0.68rem;color:#94a3b8;text-align:center;letter-spacing:0.05em;">'
        'RED SAN PABLO · ADULTO 30-59 AÑOS<br>© 2026 AUDITORÍA FINAL</p>',
        unsafe_allow_html=True
    )

# ─── ENCABEZADO ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-header">
    <div class="hero-tag">Red San Pablo · 2026</div>
    <h1 class="hero-title">SEGUIMIENTO DE PAQUETE <span>ADULTO</span></h1>
    <p class="hero-sub">Población objetivo: 30 – 59 años · 30 indicadores evaluados</p>
</div>
<div class="divider"></div>
""", unsafe_allow_html=True)

# ─── PREPARAR Y FILTRAR DATOS ────────────────────────────────────────────────
df_base = df_raw.with_columns([
    pl.col("Fecha_Atencion").cast(pl.Date),
    pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"),
    pl.col("Fecha_Atencion").dt.strftime("%B").alias("Mes_Nombre"),
    pl.col("Nombre_Establecimiento").str.strip_chars(),
    # Limpieza estricta de códigos: strip + uppercase para evitar diferencias de capitalización
    pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("Codigo_Item"),
    pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
]).with_columns(pl.col("Mes_Nombre").replace(MESES_ES))

# Filtro edad adulto
df_base = df_base.filter(
    (pl.col("Anio_Actual_Paciente") >= 30) & (pl.col("Anio_Actual_Paciente") <= 59)
)
if sel_ipress:
    df_base = df_base.filter(pl.col("Nombre_Establecimiento").is_in(sel_ipress))
if sel_mes:
    df_base = df_base.filter(pl.col("Mes_Nombre").is_in(sel_mes))
if sel_dni:
    df_base = df_base.filter(
        pl.col("Numero_Documento_Paciente").cast(pl.Utf8).str.contains(sel_dni)
    )

# ─── PROCESAMIENTO ────────────────────────────────────────────────────────────
if not df_base.is_empty():

    # Agregar columna Profesional y Paciente unificada
    df_base = df_base.with_columns([
        (pl.col("Apellido_Paterno_Personal") + " " +
         pl.col("Apellido_Materno_Personal") + " " +
         pl.col("Nombres_Personal")).alias("Profesional"),
        (pl.col("Apellido_Paterno_Paciente") + " " +
         pl.col("Apellido_Materno_Paciente") + " " +
         pl.col("Nombres_Paciente")).alias("Paciente"),
    ])

    # Datos demográficos: última atención por paciente
    cols_demo = [
        "Numero_Documento_Paciente", "Paciente",
        "Anio_Actual_Paciente", "Genero", "Descripcion_Financiador",
        "Nombre_Establecimiento", "Lote", "Num_Pag", "Num_Reg",
        "Fecha_Atencion", "Profesional"
    ]
    df_info = (
        df_base
        .sort("Fecha_Atencion", descending=True)
        .unique(subset=["Numero_Documento_Paciente"], keep="first")
        .select(cols_demo)
        .with_columns(pl.col("Fecha_Atencion").dt.strftime("%d/%m/%Y"))
    )

    # Convertir a pandas para construcción item por item (exacta y sin ambigüedad)
    df_pd = df_base.to_pandas()
    # Normalizar códigos en pandas también
    df_pd["Codigo_Item"] = df_pd["Codigo_Item"].astype(str).str.strip().str.upper()
    df_pd["Valor_Lab"]   = df_pd["Valor_Lab"].astype(str).str.strip().fillna("")
    df_pd["Numero_Documento_Paciente"] = df_pd["Numero_Documento_Paciente"].astype(str)
    df_pd["Fecha_Atencion"] = pd.to_datetime(df_pd["Fecha_Atencion"])

    # ── FUNCIÓN AUXILIAR: Obtener la celda de fecha+valor para cada ítem ──────
    # Para cada ítem filtramos exactamente los registros que corresponden,
    # tomamos el más reciente por paciente y armamos "dd/mm/yyyy (valor)" o "dd/mm/yyyy"
    def extraer_item(df: pd.DataFrame, cod_item: str, id_col: str) -> dict:
        """
        Retorna dict {DNI_str: "dd/mm/yyyy (Valor_Lab)"} para todos los pacientes
        que tienen ese ítem registrado.
        """
        cod_upper = cod_item.strip().upper()

        if id_col == "99801_TA":
            # PLAN ELABORADO → Valor_Lab = "1"
            mask = (df["Codigo_Item"] == cod_upper) & (df["Valor_Lab"] == "1")
        elif id_col == "99801_1":
            # PLAN EJECUTADO → Valor_Lab = "TA"
            mask = (df["Codigo_Item"] == cod_upper) & (df["Valor_Lab"] == "TA")
        else:
            mask = df["Codigo_Item"] == cod_upper

        sub = df[mask].copy()
        if sub.empty:
            return {}

        # Registro más reciente por paciente
        sub = sub.sort_values("Fecha_Atencion", ascending=False)
        sub = sub.drop_duplicates(subset=["Numero_Documento_Paciente"], keep="first")

        resultado = {}
        for _, row in sub.iterrows():
            fecha = row["Fecha_Atencion"].strftime("%d/%m/%Y")
            vlab  = str(row["Valor_Lab"]).strip()
            if vlab and vlab not in ("", "nan", "None", "NaN"):
                celda = f"{fecha} ({vlab})"
            else:
                celda = fecha
            resultado[str(row["Numero_Documento_Paciente"])] = celda

        return resultado

    # Construir mapa de resultados por ítem
    resultado_items = {
        id_col: extraer_item(df_pd, cod_item, id_col)
        for cod_item, id_col, _ in ITEMS_CONFIG
    }

    # ── ARMAR DATAFRAME FINAL ─────────────────────────────────────────────────
    df_final = df_info.to_pandas()
    df_final["Numero_Documento_Paciente"] = df_final["Numero_Documento_Paciente"].astype(str)

    ids_pivot    = [c[1] for c in ITEMS_CONFIG]
    mapeo_visual = {c[1]: c[2] for c in ITEMS_CONFIG}

    # Asignar cada ítem usando el mapa exacto por DNI
    for id_col in ids_pivot:
        mapa = resultado_items.get(id_col, {})
        df_final[id_col] = df_final["Numero_Documento_Paciente"].map(mapa)  # NaN si no cumple

    # Renombrar a etiquetas visuales
    df_final = df_final.rename(columns=mapeo_visual)
    columnas_indicadores = list(mapeo_visual.values())

    # Calcular avance ANTES de rellenar con ❌
    df_final["Realizados"] = df_final[columnas_indicadores].notna().sum(axis=1)
    df_final["Faltan"]     = N_ITEMS - df_final["Realizados"]
    df_final["Avance %"]   = ((df_final["Realizados"] / N_ITEMS) * 100).round(1)

    # Ahora sí rellenar con ❌ los no realizados
    df_final[columnas_indicadores] = df_final[columnas_indicadores].fillna("❌")

    # Orden de columnas — Paciente y Profesional unificados en una sola columna c/u
    columnas_datos = [
        "Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg",
        "Nombre_Establecimiento", "Numero_Documento_Paciente",
        "Paciente", "Anio_Actual_Paciente", "Genero",
        "Descripcion_Financiador", "Profesional"
    ]
    nuevo_orden = columnas_datos + ["Realizados", "Faltan", "Avance %"] + columnas_indicadores
    df_final = df_final[nuevo_orden]
    df_final = df_final.sort_values("Avance %", ascending=False).reset_index(drop=True)
    df_final.index = df_final.index + 1

    # ── MÉTRICAS ──────────────────────────────────────────────────────────────
    total       = len(df_final)
    completos   = len(df_final[df_final["Avance %"] >= 99.9])
    en_proceso  = len(df_final[(df_final["Avance %"] >= 50) & (df_final["Avance %"] < 99.9)])
    criticos    = len(df_final[df_final["Avance %"] < 50])
    avance_prom = df_final["Avance %"].mean()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">👥</div>
            <div class="metric-label">Total Pacientes</div>
            <div class="metric-value">{total}</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">✅</div>
            <div class="metric-label">Completos 100%</div>
            <div class="metric-value green">{completos}</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">🔄</div>
            <div class="metric-label">En Proceso ≥50%</div>
            <div class="metric-value yellow">{en_proceso}</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">⚠️</div>
            <div class="metric-label">Críticos &lt;50%</div>
            <div class="metric-value red">{criticos}</div>
        </div>""", unsafe_allow_html=True)
    with col5:
        color_prom = "#059669" if avance_prom >= 70 else "#d97706" if avance_prom >= 40 else "#dc2626"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">📊</div>
            <div class="metric-label">Avance Promedio</div>
            <div class="metric-value" style="color:{color_prom};">{avance_prom:.1f}%</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── TABLA ─────────────────────────────────────────────────────────────────
    styled = (
        df_final.style
        .format({"Avance %": "{:.1f}"})
        .map(
            lambda x: "color: #dc2626; font-weight:600;" if x == "❌"
                      else "color: #059669; font-weight:600;",
            subset=columnas_indicadores
        )
        .map(
            lambda val: (
                "background-color: #dcfce7; color: #166534; font-weight:700;" if val >= 99.9
                else "background-color: #fef9c3; color: #854d0e; font-weight:700;" if val >= 50
                else "background-color: #fee2e2; color: #991b1b; font-weight:700;"
            ),
            subset=["Avance %"]
        )
    )

    st.dataframe(
        styled,
        use_container_width=True,
        height=680,
        column_config={
            "Paciente": st.column_config.TextColumn(
                "Paciente",
                width="large",
                pinned=True,          # columna fija al scroll horizontal
            ),
            "Numero_Documento_Paciente": st.column_config.TextColumn(
                "DNI",
                width="small",
                pinned=True,          # DNI también fijo junto al nombre
            ),
            "Avance %": st.column_config.NumberColumn(
                "Avance %",
                format="%.1f",
                width="small",
            ),
            "Realizados": st.column_config.NumberColumn(width="small"),
            "Faltan":     st.column_config.NumberColumn(width="small"),
            "Profesional": st.column_config.TextColumn(width="large"),
        },
        selection_mode="single-row",   # activa el resaltado de fila al hacer clic
    )

    # ── DESCARGA ──────────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    col_dl, col_info = st.columns([2, 3])
    with col_dl:
        csv = df_final.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Descargar Reporte CSV",
            data=csv,
            file_name="auditoria_adulto_san_pablo_2026.csv",
            mime="text/csv",
            use_container_width=True
        )
    with col_info:
        st.markdown(
            f'<p style="color:#64748b;font-size:0.78rem;padding-top:12px;">'
            f'Reporte con <b style="color:#0369a1;">{total}</b> pacientes · '
            f'Filtro: <b style="color:#0369a1;">{"Todos los meses" if not sel_mes else ", ".join(sel_mes)}</b>'
            f'</p>',
            unsafe_allow_html=True
        )

else:
    st.markdown("""
    <div class="no-data">
        <div class="no-data-icon">⚙️</div>
        <div class="no-data-title">Configure los filtros para visualizar los datos</div>
        <p style="color:#64748b;font-size:0.85rem;margin-top:8px;">Use el panel lateral para seleccionar IPRESS y período de atención.</p>
    </div>
    """, unsafe_allow_html=True)
