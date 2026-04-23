import streamlit as st
import polars as pl
import pandas as pd
import os

# 1. Configuración inmediata
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. Localizar el archivo de forma segura
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

# 3. Función de carga que no consume RAM (Lazy Loading)
@st.cache_data
def obtener_establecimientos():
    if not os.path.exists(ARCHIVO_PARQUET): return []
    try:
        # scan_parquet no lee el archivo, solo mira los nombres
        return pl.scan_parquet(ARCHIVO_PARQUET).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()
    except: return []

# 4. Sidebar: Filtro obligatorio para ahorrar memoria
st.sidebar.header("Filtros")
lista_ipress = obtener_establecimientos()
target_default = "SAN LUIS BAJO - GRANDE"
default_val = [i for i in lista_ipress if target_default in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=lista_ipress, default=default_val)

if not sel_ipress:
    st.info("👈 Por seguridad del servidor, selecciona una IPRESS para cargar los datos.")
    st.stop()

# 5. Procesamiento ultra-eficiente
@st.cache_data(ttl=600)
def procesar_seguro(ipress_list):
    try:
        # Usamos scan_parquet para filtrar ANTES de cargar a memoria
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        # Filtros básicos (Edad y Establecimiento)
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_list)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Traemos solo columnas que usaremos para el pivot
        columnas = ["Fecha_Atencion", "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                    "Apellido_Materno_Paciente", "Nombres_Paciente", "Codigo_Item", "Valor_Lab"]
        
        # collect() es el único momento donde los datos entran a la RAM, pero ya filtrados
        df = lf.select(columnas).collect()
        
        if df.is_empty(): return None

        # Limpieza rápida
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null("")
        ])

        # Crear IDs específicos para pivotar
        df = df.with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID_Col")
        )
        return df
    except: return None

df_f = procesar_seguro(sel_ipress)

if df_f is None:
    st.warning("No se encontraron datos.")
    st.stop()

# 6. Pivotado y visualización
items_auditoria = [
    "99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
    "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
    "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
    "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"
]

# Pivotamos nativo en Polars
df_piv = df_f.filter(pl.col("ID_Col").is_in(items_auditoria)).pivot(
    values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID_Col", aggregate_function="first"
)

# Datos de identificación
df_id = df_f.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                     "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique()

# Resultado final
final = df_id.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()

for col in items_auditoria:
    if col not in final.columns: final[col] = None

final["Avance %"] = (final[items_auditoria].notna().sum(axis=1) / 30 * 100).round(1)
final = final.fillna("❌").sort_values("Avance %", ascending=False)

st.title("📊 Seguimiento Adulto")
st.dataframe(final, use_container_width=True)
