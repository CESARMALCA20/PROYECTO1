import streamlit as st
import os

# 1. ESTABILIZACIÓN DE PÁGINA (Previene el parpadeo de carga)
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# Título estático (no depende de datos, así que no debería crashear)
st.markdown('<h2 style="color:#38bdf8;">📊 Auditoría San Pablo - Paquete Adulto</h2>', unsafe_allow_html=True)

# 2. DEFINICIÓN DE RUTAS SIN CARGAR LIBRERÍAS PESADAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
PATH_DATA = os.path.join(BASE_DIR, "data", "reporte.parquet")

# 3. INTERFAZ DE AISLAMIENTO
# Si la página se reinicia, este estado se pierde y vuelve al botón inicial, evitando el bucle infinito de crash.
if "ready" not in st.session_state:
    st.session_state.ready = False

if not st.session_state.ready:
    st.warning("⚠️ La base de datos es pesada para el servidor.")
    if st.button("🚀 Iniciar Auditoría (Cargar RAM)"):
        st.session_state.ready = True
        st.rerun()
    st.stop()

# 4. CARGA DIFERIDA DE LIBRERÍAS (Solo si el usuario presionó el botón)
try:
    import polars as pl
    import pandas as pd
except ImportError:
    st.error("Faltan librerías en requirements.txt")
    st.stop()

# 5. PROCESAMIENTO QUIRÚRGICO CON LAZY LOADING
@st.cache_data(ttl=300)
def procesar_paquete_adulto(establecimientos):
    try:
        # SCAN es la clave: no lee el archivo, solo crea un mapa mental
        lf = pl.scan_parquet(PATH_DATA)
        
        # Filtros directos al archivo (muy eficiente)
        lf = lf.filter(
            (pl.col("Nombre_Establecimiento").is_in(establecimientos)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        )
        
        # Seleccionamos solo lo mínimo indispensable
        df = lf.select([
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
            "Apellido_Materno_Paciente", "Nombres_Paciente", 
            "Codigo_Item", "Valor_Lab", "Fecha_Atencion", "Nombre_Establecimiento"
        ]).collect() # Aquí es donde realmente entra a la RAM, pero ya filtrado
        
        if df.is_empty(): return None

        # Preparar IDs de columna
        df = df.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
            pl.col("Fecha_Atencion").cast(pl.Date)
        ]).with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("COL_ID")
        )
        return df
    except Exception as e:
        st.error(f"Error interno: {e}")
        return None

# Sidebar para elegir IPRESS (Carga solo una vez)
@st.cache_data
def get_ipress():
    return pl.scan_parquet(PATH_DATA).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()

try:
    lista_ipress = get_ipress()
    sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress)

    if sel_ipress:
        with st.spinner("Filtrando registros..."):
            df_final = procesar_paquete_adulto(sel_ipress)
            
            if df_final is not None:
                # Los 30 ítems
                items = ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
                         "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
                         "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
                         "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"]

                # Pivotado y limpieza
                df_p = df_final.filter(pl.col("COL_ID").is_in(items)).pivot(
                    values="Fecha_Atencion", index="Numero_Documento_Paciente", on="COL_ID", aggregate_function="first"
                )
                
                df_id = df_final.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                                         "Apellido_Materno_Paciente", "Nombres_Paciente", "Nombre_Establecimiento"]).unique()
                
                res = df_id.join(df_p, on="Numero_Documento_Paciente", how="left").to_pandas()
                
                for c in items:
                    if c not in res.columns: res[c] = None
                
                res["Avance %"] = (res[items].notna().sum(axis=1) / 30 * 100).round(1)
                res = res.fillna("❌").sort_values("Avance %", ascending=False)

                st.dataframe(res, use_container_width=True, height=600)
            else:
                st.info("No se encontraron registros para esta selección.")
    else:
        st.info("👈 Seleccione una IPRESS en el lateral para comenzar.")

except Exception as e:
    st.error(f"Error crítico de carga: {e}")

if st.button("🗑️ Liberar Memoria"):
    st.session_state.ready = False
    st.rerun()
