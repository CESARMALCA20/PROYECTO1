import streamlit as st
import os

# 1. ESTABILIZACIÓN INICIAL
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. RUTAS (Sin cargar librerías aún)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

st.title("🏥 Auditoría de Paquete Adulto")

# 3. INTERRUPTOR DE SEGURIDAD (Evita el crash automático al entrar)
if "conexion_activa" not in st.session_state:
    st.session_state.conexion_activa = False

if not st.session_state.conexion_activa:
    st.info("La base de datos es pesada para la nube. Presione el botón para iniciar la conexión.")
    if st.button("🔌 Conectar con Base de Datos"):
        st.session_state.conexion_activa = True
        st.rerun()
    st.stop()

# --- SI EL BOTÓN SE PRESIONÓ, RECIÉN AQUÍ CARGAMOS LIBRERÍAS ---
import polars as pl
import pandas as pd

@st.cache_data(ttl=600)
def listar_ipress_sin_memoria():
    try:
        # scan_parquet es virtual, casi no gasta RAM
        return pl.scan_parquet(ARCHIVO_PARQUET).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()
    except Exception as e:
        st.error(f"Error al leer: {e}")
        return []

lista_ipress = listar_ipress_sin_memoria()
sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=lista_ipress)

if not sel_ipress:
    st.warning("👈 Seleccione una IPRESS en el menú lateral para procesar.")
    if st.button("❌ Cerrar Conexión"):
        st.session_state.conexion_activa = False
        st.rerun()
    st.stop()

# 4. PROCESAMIENTO QUIRÚRGICO (Solo para la IPRESS elegida)
@st.cache_data(ttl=300)
def procesar_auditoria_ligera(ipress_list):
    try:
        # Filtramos directamente en el archivo antes de subirlo a la RAM
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_list)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Solo traemos columnas indispensables
        cols = ["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
                "Nombres_Paciente", "Codigo_Item", "Valor_Lab", "Fecha_Atencion"]
        
        df = lf.select(cols).collect()
        
        if df.is_empty(): return None

        # Limpieza y formateo
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
            pl.col("Fecha_Atencion").cast(pl.Date)
        ])

        # Crear IDs para el Pivot
        df = df.with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID")
        )
        return df
    except: return None

with st.spinner("Procesando datos..."):
    df_f = procesar_auditoria_ligera(sel_ipress)

if df_f is not None:
    indicadores = ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
                  "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
                  "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
                  "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"]

    # Pivotado nativo Polars
    df_p = df_f.filter(pl.col("ID").is_in(indicadores)).pivot(
        values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID", aggregate_function="first"
    )
    
    df_id = df_f.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                         "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique()
    
    # Unión y conversión final a Pandas para mostrar
    res = df_id.join(df_p, on="Numero_Documento_Paciente", how="left").to_pandas()
    
    for c in indicadores:
        if c not in res.columns: res[c] = None
    
    res["Avance %"] = (res[indicadores].notna().sum(axis=1) / 30 * 100).round(1)
    res = res.fillna("❌").sort_values("Avance %", ascending=False)

    st.dataframe(res, use_container_width=True)
else:
    st.warning("No se encontraron registros para esta selección.")
