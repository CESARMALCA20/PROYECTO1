import streamlit as st
import polars as pl
import pandas as pd
from pathlib import Path
import os

# 1. CONFIGURACIÓN DE PÁGINA (Debe ser lo primero)
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. DEFINICIÓN DE RUTAS ABSOLUTAS
# Nos aseguramos de encontrar el archivo sin importar dónde se ejecute
current_dir = Path(__file__).parent
root_dir = current_dir.parent if current_dir.name == "pages" else current_dir
path_parquet = root_dir / "data" / "reporte.parquet"

# 3. CARGA DE METADATOS (Mínimo consumo de RAM)
@st.cache_data(ttl=3600)
def get_ipress_list():
    if not path_parquet.exists():
        return []
    try:
        # scan_parquet no lee el archivo, solo lo mapea. Muy ligero.
        q = pl.scan_parquet(str(path_parquet)).select("Nombre_Establecimiento").unique().sort("Nombre_Establecimiento")
        return q.collect()["Nombre_Establecimiento"].to_list()
    except:
        return []

# 4. SIDEBAR - SELECCIÓN OBLIGATORIA
st.sidebar.header("Filtros de Auditoría")
opciones = get_ipress_list()

# Intentar pre-seleccionar San Luis Bajo Grande
default_ipress = [i for i in opciones if "SAN LUIS BAJO - GRANDE" in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=opciones, default=default_ipress)

# 5. BLOQUEO DE SEGURIDAD
if not sel_ipress:
    st.warning("⚠️ Selecciona una IPRESS en el menú lateral para activar la auditoría.")
    st.info("Esto evita que el servidor colapse por falta de memoria RAM.")
    st.stop()

# 6. PROCESAMIENTO FILTRADO (Solo para la IPRESS elegida)
@st.cache_data(ttl=600)
def procesar_data_adulta(ipress_list):
    try:
        # Solo cargamos a RAM lo que filtró el scan
        lf = pl.scan_parquet(str(path_parquet))
        
        # Filtros críticos antes de collect()
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_list)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Seleccionamos solo columnas vitales para ahorrar espacio
        cols = ["Fecha_Atencion", "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                "Apellido_Materno_Paciente", "Nombres_Paciente", "Anio_Actual_Paciente", 
                "Codigo_Item", "Valor_Lab", "Nombre_Establecimiento"]
        
        df = lf.select(cols).collect()
        
        # Formatear datos
        df = df.with_columns([
            pl.col("Fecha_Atencion").cast(pl.Date),
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null("")
        ])
        
        # Crear ID de Columna
        df = df.with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID_Col")
        )
        
        return df
    except Exception as e:
        return str(e)

# Ejecutar proceso
data_f = procesar_data_adulta(sel_ipress)

if isinstance(data_f, str):
    st.error(f"Error técnico: {data_f}")
    st.stop()

if data_f.is_empty():
    st.info("No hay registros adultos para esta selección.")
    st.stop()

# 7. PIVOTADO Y TABLA FINAL
codigos_paquete = [
    "99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
    "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
    "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
    "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"
]

df_pivot = data_f.filter(pl.col("ID_Col").is_in(codigos_paquete)).pivot(
    values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID_Col", aggregate_function="first"
)

# Unir con datos personales
df_pers = data_f.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                         "Apellido_Materno_Paciente", "Nombres_Paciente", "Anio_Actual_Paciente"]).unique()

res = df_pers.join(df_pivot, on="Numero_Documento_Paciente", how="left").to_pandas()

# Limpieza final para visualización
for c in codigos_paquete:
    if c not in res.columns: res[c] = None

res["Avance %"] = (res[codigos_paquete].notna().sum(axis=1) / 30 * 100).round(1)
res = res.fillna("❌")

st.header("📊 Auditoría Adulto")
st.dataframe(res, use_container_width=True)
