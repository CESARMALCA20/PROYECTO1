import streamlit as st
import os

# 1. Configuración de página (Debe ser lo primero)
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. Localizar el archivo de forma segura
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

# Título de la página
st.markdown('<h2 style="color:#38bdf8;">Paquete Adulto - Red San Pablo</h2>', unsafe_allow_html=True)

# 3. Solo importamos polars cuando es necesario para ahorrar memoria inicial
import polars as pl

@st.cache_data(ttl=600)
def obtener_lista_ipress():
    if not os.path.exists(ARCHIVO_PARQUET):
        return []
    # scan_parquet NO carga el archivo a la RAM, solo lo mapea (Ultra ligero)
    lf = pl.scan_parquet(ARCHIVO_PARQUET)
    return lf.select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()

# 4. Sidebar: Selección obligatoria
st.sidebar.header("Filtros")
opciones = obtener_lista_ipress()

# Buscamos la IPRESS por defecto de forma segura
target = "SAN LUIS BAJO - GRANDE"
default_sel = [i for i in opciones if target in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=opciones, default=default_sel)

# --- BLOQUEO DE SEGURIDAD ---
if not sel_ipress:
    st.info("👈 Seleccione una IPRESS en el menú lateral para mostrar los datos. \n\nEsto evita que el servidor de la nube colapse por falta de memoria.")
    st.stop()
# ----------------------------

# 5. Procesamiento optimizado (Solo se ejecuta si hay una IPRESS seleccionada)
@st.cache_data(ttl=300)
def procesar_datos_adulto(ipress_list):
    try:
        # Volvemos a usar scan_parquet para filtrar ANTES de traer los datos a la RAM
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        # Filtros críticos
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(ipress_list)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Traemos solo las columnas necesarias para el reporte
        columnas_vitales = [
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
            "Nombres_Paciente", "Codigo_Item", "Valor_Lab", "Fecha_Atencion"
        ]
        
        df = lf.select(columnas_vitales).collect()
        
        if df.is_empty():
            return None

        # Limpieza y preparación de IDs para pivotar
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
            pl.col("Fecha_Atencion").cast(pl.Date)
        ])

        df = df.with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID_AUDITORIA")
        )
        return df
    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

# Ejecución
with st.spinner("Cargando registros..."):
    df_resultado = procesar_datos_adulto(sel_ipress)

if df_resultado is not None:
    # Definición de indicadores (30 ítems)
    indicadores = [
        "99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
        "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
        "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
        "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"
    ]

    # Pivotado
    df_piv = df_resultado.filter(pl.col("ID_AUDITORIA").is_in(indicadores)).pivot(
        values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID_AUDITORIA", aggregate_function="first"
    )
    
    # Datos de identificación
    df_id = df_resultado.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                                 "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique()
    
    # Unión final (usando pandas solo para la visualización)
    import pandas as pd
    res = df_id.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()
    
    # Asegurar que todas las columnas existan
    for col in indicadores:
        if col not in res.columns: res[col] = None
    
    res["Avance %"] = (res[indicadores].notna().sum(axis=1) / 30 * 100).round(1)
    res = res.fillna("❌").sort_values("Avance %", ascending=False)

    st.dataframe(res, use_container_width=True)
else:
    st.warning("No se encontraron datos para los filtros seleccionados.")
