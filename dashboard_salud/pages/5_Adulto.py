import streamlit as st
import polars as pl
import pandas as pd
import os

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(layout="wide", page_title="Auditoría Adulto - San Pablo")

# ─── 1. RUTAS Y CARGA LIGERA ────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)

ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

@st.cache_data
def obtener_ipress_disponibles():
    """Solo lee la columna de IPRESS para no saturar la RAM"""
    if not os.path.exists(ARCHIVO_PARQUET): return []
    try:
        # scan_parquet no carga el archivo, solo lo mapea
        df_min = pl.scan_parquet(ARCHIVO_PARQUET).select("Nombre_Establecimiento").collect()
        return sorted(df_min["Nombre_Establecimiento"].unique().to_list())
    except:
        return []

# ─── 2. SIDEBAR (FILTROS) ───────────────────────────────────────────────────
st.sidebar.header("Filtros de Auditoría")

lista_ipress = obtener_ipress_disponibles()
target = "SAN LUIS BAJO - GRANDE"
default_val = [i for i in lista_ipress if target in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress, default=default_val)

# Detener si no hay selección para evitar que el servidor trabaje de más
if not sel_ipress:
    st.info("👈 Por favor, seleccione una IPRESS en el menú lateral para cargar los datos.")
    st.stop()

# ─── 3. PROCESAMIENTO CON LAZY LOADING (PARA EVITAR CRASH) ──────────────────
@st.cache_data(ttl=600) # El cache dura 10 min para liberar RAM seguido
def procesar_auditoria(ipress_seleccionadas):
    try:
        # Usamos SCAN para filtrar ANTES de que los datos toquen la RAM
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        # Filtros inmediatos: IPRESS y Edad (30-59 años)
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_seleccionadas)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Solo traer las columnas necesarias
        cols_necesarias = [
            "Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Nombre_Establecimiento",
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente",
            "Nombres_Paciente", "Anio_Actual_Paciente", "Codigo_Item", "Valor_Lab", "Profesional"
        ]
        # Verificar si las columnas existen y seleccionarlas
        df = lf.select([pl.col(c) for c in cols_necesarias if c in lf.columns]).collect()
        
        # Limpiar espacios
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
            pl.col("Fecha_Atencion").cast(pl.Date)
        ])
        
        return df
    except Exception as e:
        return str(e)

df_f = procesar_auditoria(sel_ipress)

if isinstance(df_f, str):
    st.error(f"Error al procesar: {df_f}")
    st.stop()

if df_f.is_empty():
    st.warning("No se encontraron registros para los filtros seleccionados.")
    st.stop()

# ─── 4. LÓGICA DE PIVOTADO Y VISUALIZACIÓN ──────────────────────────────────
# Definición de los 30 ítems (simplificado para evitar exceso de proceso)
items_interes = [
    "99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
    "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
    "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
    "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"
]

# Crear columna ID para pivotar
df_f = df_f.with_columns(
    pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
    .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
    .otherwise(pl.col("Codigo_Item")).alias("ID_Col")
)

# Filtrar solo lo que vamos a mostrar
df_items = df_f.filter(pl.col("ID_Col").is_in(items_interes))

if not df_items.is_empty():
    # Pivotar nativo (Polars)
    df_piv = df_items.pivot(
        values="Fecha_Atencion", 
        index="Numero_Documento_Paciente", 
        on="ID_Col", 
        aggregate_function="first"
    )
    
    # Info de pacientes
    df_pac = df_f.select([
        "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
        "Nombres_Paciente", "Anio_Actual_Paciente"
    ]).unique(subset=["Numero_Documento_Paciente"])
    
    # Unión y conversión a Pandas para mostrar
    df_final = df_pac.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()
    
    # Asegurar que todas las columnas existan
    for col in items_interes:
        if col not in df_final.columns: df_final[col] = None
    
    # Cálculos finales
    df_final["Realizados"] = df_final[items_interes].notna().sum(axis=1)
    df_final["Avance %"] = (df_final["Realizados"] / 30 * 100).round(1)
    df_final = df_final.fillna("❌")

    st.markdown("### 📊 Seguimiento de Paquete Adulto (30-59 años)")
    st.dataframe(df_final, use_container_width=True)
