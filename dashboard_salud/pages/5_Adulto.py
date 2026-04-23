import streamlit as st
import os

# 1. CONFIGURACIÓN DE PÁGINA (Debe ser lo primero)
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. LOCALIZAR ARCHIVO SIN CARGARLO
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

st.title("📊 Seguimiento Paquete Adulto")

# 3. CONTROL DE ACCESO (Evita que el servidor colapse al entrar)
if "conectado" not in st.session_state:
    st.session_state.conectado = False

if not st.session_state.conectado:
    st.info("La base de datos es grande. Presione el botón para conectar de forma segura.")
    if st.button("🔗 Conectar a Base de Datos"):
        st.session_state.conectado = True
        st.rerun()
    st.stop()

# --- SI LLEGAMOS AQUÍ, ES QUE PRESIONASTE EL BOTÓN ---

import polars as pl

@st.cache_data
def obtener_establecimientos():
    try:
        # scan_parquet es virtual, no gasta RAM real hasta el collect()
        return pl.scan_parquet(ARCHIVO_PARQUET).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()
    except Exception as e:
        st.error(f"Error al leer archivo: {e}")
        return []

lista_ipress = obtener_establecimientos()
sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=lista_ipress)

if not sel_ipress:
    st.warning("👈 Seleccione una IPRESS en el menú lateral para procesar los datos.")
    if st.button("🔌 Desconectar para liberar RAM"):
        st.session_state.conectado = False
        st.rerun()
    st.stop()

# 4. PROCESAMIENTO QUIRÚRGICO (Solo para la IPRESS elegida)
@st.cache_data(ttl=300)
def procesar_auditoria(ipress_list):
    try:
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        # Filtros directos al archivo (Lazy)
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_list)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Solo columnas vitales
        df = lf.select([
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
            "Apellido_Materno_Paciente", "Nombres_Paciente", 
            "Codigo_Item", "Valor_Lab", "Fecha_Atencion"
        ]).collect()
        
        if df.is_empty(): return None

        # Preparar IDs para Pivot
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
            pl.col("Fecha_Atencion").cast(pl.Date)
        ]).with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID")
        )
        return df
    except: return None

with st.spinner("Procesando IPRESS seleccionada..."):
    df_f = procesar_auditoria(sel_ipress)

if df_f is not None:
    items = ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
             "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
             "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
             "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"]

    # Pivotado nativo
    df_p = df_f.filter(pl.col("ID").is_in(items)).pivot(
        values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID", aggregate_function="first"
    )
    
    # Union con nombres
    df_id = df_f.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                         "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique()
    
    import pandas as pd
    res = df_id.join(df_p, on="Numero_Documento_Paciente", how="left").to_pandas()
    
    for c in items:
        if c not in res.columns: res[c] = None
    
    res["Avance %"] = (res[items].notna().sum(axis=1) / 30 * 100).round(1)
    res = res.fillna("❌").sort_values("Avance %", ascending=False)

    st.dataframe(res, use_container_width=True)
else:
    st.warning("No hay datos para esta selección.")

if st.button("🔴 Cerrar sesión de datos (Liberar memoria)"):
    st.session_state.conectado = False
    st.rerun()
