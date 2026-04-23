import streamlit as st
import polars as pl
import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# ─── 1. CARGA DE DATOS OPTIMIZADA ──────────────────────────────────────────
@st.cache_data(ttl=3600) # El caché vence cada hora para liberar memoria
def cargar_data():
    try:
        # Localización robusta del archivo
        ruta = Path(__file__).resolve().parent.parent / "data" / "reporte.parquet"
        
        if not ruta.exists():
            return None
            
        # Leemos solo las columnas necesarias para ahorrar RAM
        # Si el error persiste, es probable que el archivo sea demasiado grande para el servidor gratuito
        df = pl.read_parquet(str(ruta), use_pyarrow=True)
        
        # Limpieza de nombres de columnas
        df = df.rename({c: c.strip() for c in df.columns})
        
        # Procesar fechas y meses de forma nativa (más rápido que map_elements)
        df = df.with_columns([
            pl.col("Fecha_Atencion").cast(pl.Date),
            pl.col("Anio_Actual_Paciente").cast(pl.Int32)
        ])
        
        df = df.with_columns(pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"))
        
        return df
    except Exception as e:
        st.error(f"Error al cargar: {e}")
        return None

df = cargar_data()

if df is None:
    st.error("No se encontró el archivo de datos.")
    st.stop()

# ─── 2. FILTROS PREVIOS (PARA NO SOBRECARGAR LA MEMORIA) ───────────────────
st.sidebar.header("Filtros")

# Filtro de Edad inmediato
df = df.filter((pl.col("Anio_Actual_Paciente") >= 30) & (pl.col("Anio_Actual_Paciente") <= 59))

lista_ipress = sorted(df["Nombre_Establecimiento"].unique().to_list())
sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress, default=[])

if not sel_ipress:
    st.info("⚠️ Selecciona una IPRESS en el menú lateral para cargar los datos. Esto evita que la página colapse por exceso de información.")
    st.stop()

# Filtrar por IPRESS antes de seguir procesando
df = df.filter(pl.col("Nombre_Establecimiento").is_in(sel_ipress))

# ─── 3. LÓGICA DE PIVOTADO OPTIMIZADA ──────────────────────────────────────
# Definimos los ítems (Codigo, ID_Final)
config = {
    "99401": "99401", "Z019": "Z019", "Z017": "Z017", "99209.02": "99209.02",
    "99209.03": "99209.03", "99199.22": "99199.22", "96150.02": "96150.02",
    "96150.03": "96150.03", "99402.09": "99402.09", "99173": "99173",
    "99401.16": "99401.16", "99401.33": "99401.33", "86703.01": "86703.01",
    "86318.01": "86318.01", "99401.34": "99401.34", "D0150": "D0150",
    "99402.03": "99402.03", "90688": "90688", "Z030": "Z030",
    "99199.58": "99199.58", "87342": "87342", "88141.01": "88141.01",
    "84152": "84152", "82270": "82270", "Z128": "Z128", "99401.12": "99401.12"
}

# Preparar columna de contenido (Fecha + Lab si existe)
df = df.with_columns(
    pl.format("{} ({})", 
              pl.col("Fecha_Atencion").dt.strftime("%d/%m/%Y"), 
              pl.col("Valor_Lab").fill_null("")
             ).str.replace(r" \(\)$", "").alias("Contenido")
)

# Crear IDs específicos para los casos especiales (99801 y 96150.01)
df = df.with_columns(
    pl.when(pl.col("Codigo_Item") == "99801")
    .then(pl.format("99801_{}", pl.col("Valor_Lab")))
    .when(pl.col("Codigo_Item") == "96150.01")
    .then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
    .otherwise(pl.col("Codigo_Item"))
    .alias("ID_Col")
)

# Solo nos quedamos con los códigos que nos interesan
codigos_interes = list(config.keys()) + ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES"]
df_items = df.filter(pl.col("ID_Col").is_in(codigos_interes))

if not df_items.is_empty():
    # Pivotado nativo
    df_piv = df_items.pivot(
        values="Contenido", 
        index="Numero_Documento_Paciente", 
        on="ID_Col", 
        aggregate_function="first"
    )
    
    # Info básica del paciente
    df_info = df.select([
        "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
        "Apellido_Materno_Paciente", "Nombres_Paciente", "Anio_Actual_Paciente", "Profesional"
    ]).unique(subset=["Numero_Documento_Paciente"])
    
    # Unión final
    df_final = df_info.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()
    
    # Rellenar faltantes y calcular avance
    cols_check = [c for c in df_final.columns if c not in df_info.columns]
    df_final["Realizados"] = df_final[cols_check].notna().sum(axis=1)
    df_final["Avance %"] = (df_final["Realizados"] / 30 * 100).round(1)
    df_final = df_final.fillna("❌")

    st.title("Seguimiento Adulto")
    st.dataframe(df_final, use_container_width=True)
else:
    st.warning("No hay actividades registradas para esta IPRESS.")
