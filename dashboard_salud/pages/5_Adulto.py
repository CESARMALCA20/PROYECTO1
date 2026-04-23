import streamlit as st
import polars as pl
import pandas as pd
import os

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# ─── 1. DETERMINAR RUTA DEL ARCHIVO ──────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)

ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

# ─── 2. CARGA DE METADATOS (SOLO NOMBRES DE IPRESS) ──────────────────────────
@st.cache_data
def obtener_lista_ipress():
    if not os.path.exists(ARCHIVO_PARQUET):
        return []
    try:
        # Leemos solo la columna de IPRESS para no saturar la RAM
        df_min = pl.scan_parquet(ARCHIVO_PARQUET).select("Nombre_Establecimiento").collect()
        return sorted(df_min["Nombre_Establecimiento"].unique().to_list())
    except:
        return []

# ─── 3. FILTROS EN EL SIDEBAR ────────────────────────────────────────────────
st.sidebar.header("Filtros de Auditoría")
opciones_ipress = obtener_lista_ipress()

# Buscamos la IPRESS por defecto
target = "SAN LUIS BAJO - GRANDE"
default_val = [i for i in opciones_ipress if target in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=opciones_ipress, default=default_val)

if not sel_ipress:
    st.info("👈 Por favor, seleccione al menos una IPRESS en el menú lateral para cargar los datos.")
    st.stop()

# ─── 4. CARGA Y PROCESAMIENTO FILTRADO (SOLO LO NECESARIO) ───────────────────
@st.cache_data
def cargar_datos_filtrados(lista_seleccionada):
    try:
        # Usamos LazyFrame (scan_parquet) para filtrar ANTES de cargar a la RAM
        lf = pl.scan_parquet(ARCHIVO_PARQUET)
        
        # Filtros inmediatos (IPRESS y Edad Adulto)
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(lista_seleccionada)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        df = lf.collect()
        
        # Limpieza de columnas
        df = df.rename({col: col.strip() for col in df.columns})
        
        # Procesar fechas
        df = df.with_columns([
            pl.col("Fecha_Atencion").cast(pl.Date),
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null("")
        ])
        
        return df
    except Exception as e:
        st.error(f"Error al procesar datos: {e}")
        return None

df_f = cargar_datos_filtrados(sel_ipress)

# ─── 5. LÓGICA DE AUDITORÍA (30 ÍTEMS) ───────────────────────────────────────
if df_f is not None and not df_f.is_empty():
    
    # Crear ID de Columna para el pivotado
    df_f = df_f.with_columns(
        pl.when(pl.col("Codigo_Item") == "99801")
        .then(pl.format("99801_{}", pl.col("Valor_Lab")))
        .when(pl.col("Codigo_Item") == "96150.01")
        .then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
        .otherwise(pl.col("Codigo_Item"))
        .alias("ID_Col")
    )

    # Texto para la celda
    df_f = df_f.with_columns(
        pl.format("{} ({})", 
                  pl.col("Fecha_Atencion").dt.strftime("%d/%m/%Y"), 
                  pl.col("Valor_Lab")
        ).str.replace(r" \(\)$", "").alias("Celda")
    )

    # Lista de códigos que queremos ver en las columnas
    items_interes = [
        "99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
        "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
        "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
        "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"
    ]

    # Pivotado
    df_items = df_f.filter(pl.col("ID_Col").is_in(items_interes))
    
    if not df_items.is_empty():
        df_piv = df_items.pivot(values="Celda", index="Numero_Documento_Paciente", on="ID_Col", aggregate_function="first")
        
        # Info del paciente
        df_pac = df_f.select([
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
            "Nombres_Paciente", "Anio_Actual_Paciente", "Nombre_Establecimiento"
        ]).unique(subset=["Numero_Documento_Paciente"])
        
        # Unión final en Pandas para el estilo visual
        df_final = df_pac.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()
        
        # Asegurar columnas
        for col in items_interes:
            if col not in df_final.columns: df_final[col] = None
        
        df_final["Realizados"] = df_final[items_interes].notna().sum(axis=1)
        df_final["Avance %"] = (df_final["Realizados"] / 30 * 100).round(1)
        df_final = df_final.fillna("❌")

        st.title("📊 Seguimiento Adulto")
        st.dataframe(df_final, use_container_width=True)
    else:
        st.warning("No se encontraron los códigos de auditoría para esta IPRESS.")
else:
    st.warning("No hay pacientes registrados en este rango de edad para la selección.")
