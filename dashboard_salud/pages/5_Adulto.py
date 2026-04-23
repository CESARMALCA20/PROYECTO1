import streamlit as st
import os

# 1. ESTABILIZACIÓN TOTAL
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# Título estático (no consume RAM)
st.markdown('<h2 style="color:#38bdf8;">🛡️ Modo de Seguridad: Auditoría Adulto</h2>', unsafe_allow_html=True)

# 2. RUTAS DE ARCHIVO
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)
PATH_DATA = os.path.join(BASE_DIR, "data", "reporte.parquet")

# 3. INTERRUPTOR DE CARGA (Evita que el servidor indexe el archivo al entrar)
if "session_active" not in st.session_state:
    st.session_state.session_active = False

if not st.session_state.session_active:
    st.info("La base de datos es muy pesada para la memoria gratuita de la nube.")
    if st.button("🔌 ACTIVAR CONEXIÓN SEGURA"):
        st.session_state.session_active = True
        st.rerun()
    st.stop()

# 4. CARGA BAJO DEMANDA (Solo después del botón)
import polars as pl

@st.cache_data
def get_ipress_list():
    # scan_parquet es virtual, no carga el archivo a la RAM
    return pl.scan_parquet(PATH_DATA).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()

try:
    ipress_options = get_ipress_list()
    
    # Pre-selección por defecto
    target = "SAN LUIS BAJO - GRANDE"
    default_ipress = [i for i in ipress_options if target in i.upper()]
    
    sel_ipress = st.sidebar.multiselect("🏥 Elija IPRESS", options=ipress_options, default=default_ipress)

    if not sel_ipress:
        st.warning("Seleccione una IPRESS para procesar.")
        if st.button("❌ Cerrar Conexión (Liberar RAM)"):
            st.session_state.session_active = False
            st.rerun()
        st.stop()

    # 5. PROCESAMIENTO POR TROZOS (Lazy Loading)
    with st.spinner("Procesando solo los datos necesarios..."):
        # Solo pedimos las columnas que vamos a usar
        lf = pl.scan_parquet(PATH_DATA).filter(
            (pl.col("Nombre_Establecimiento").is_in(sel_ipress)) &
            (pl.col("Anio_Actual_Paciente") >= 30) &
            (pl.col("Anio_Actual_Paciente") <= 59)
        ).select([
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
            "Nombres_Paciente", "Codigo_Item", "Valor_Lab", "Fecha_Atencion"
        ])

        df = lf.collect() # Aquí es el único momento que toca la RAM

        if not df.is_empty():
            # Lógica de Pivotado simplificada
            df = df.with_columns([
                pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
                pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
                pl.col("Fecha_Atencion").cast(pl.Date)
            ]).with_columns(
                pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
                .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
                .otherwise(pl.col("Codigo_Item")).alias("ID_AUDIT")
            )

            indicadores = ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
                          "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
                          "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
                          "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"]

            df_piv = df.filter(pl.col("ID_ID_AUDIT").is_in(indicadores)).pivot(
                values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID_AUDIT", aggregate_function="first"
            )
            
            # Mostrar tabla final
            import pandas as pd
            res = df.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique().join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()
            
            for c in indicadores:
                if c not in res.columns: res[c] = None
            
            res["Avance %"] = (res[indicadores].notna().sum(axis=1) / 30 * 100).round(1)
            st.dataframe(res.fillna("❌"), use_container_width=True)
        else:
            st.info("No se hallaron registros.")

except Exception as e:
    st.error(f"Error de memoria: {e}")
