import streamlit as st
import os

# 1. CONFIGURACIÓN DE PÁGINA (Debe ser lo primerito)
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# 2. FUNCIÓN DE CARGA DENTRO DE UN TRY-EXCEPT GLOBAL
def ejecutar_aplicacion():
    import polars as pl
    import pandas as pd
    from pathlib import Path

    # Localizar archivo
    current_dir = Path(__file__).parent
    root_dir = current_dir.parent if current_dir.name == "pages" else current_dir
    path_parquet = root_dir / "data" / "reporte.parquet"

    if not path_parquet.exists():
        st.error(f"Archivo no encontrado en: {path_parquet}")
        return

    # --- SIDEBAR ---
    st.sidebar.header("Filtros de Auditoría")
    
    # Carga ultra-ligera solo para nombres de IPRESS
    try:
        @st.cache_data(ttl=600)
        def get_ipress():
            # Solo escaneamos una columna para no gastar RAM
            return pl.scan_parquet(str(path_parquet)).select("Nombre_Establecimiento").unique().collect().get_column("Nombre_Establecimiento").sort().to_list()
        
        lista_ipress = get_ipress()
    except Exception as e:
        st.error(f"Error al leer metadatos: {e}")
        return

    target = "SAN LUIS BAJO - GRANDE"
    default_ipress = [i for i in lista_ipress if target in i.upper()]

    sel_ipress = st.sidebar.multiselect("🏥 Seleccione IPRESS", options=lista_ipress, default=default_ipress)

    if not sel_ipress:
        st.info("👈 Seleccione una IPRESS para cargar los datos.")
        st.stop()

    # --- PROCESAMIENTO ---
    with st.spinner("Procesando datos..."):
        try:
            # Lazy Loading: No cargamos a RAM hasta el collect()
            lf = pl.scan_parquet(str(path_parquet))
            
            # Filtros inmediatos (IPRESS y Edad)
            lf = lf.filter(
                (pl.col("Nombre_Establecimiento").is_in(sel_ipress)) &
                (pl.col("Anio_Actual_Paciente").cast(pl.Int32) >= 30) &
                (pl.col("Anio_Actual_Paciente") <= 59)
            )
            
            # Solo las columnas necesarias
            cols_ok = ["Fecha_Atencion", "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                       "Apellido_Materno_Paciente", "Nombres_Paciente", "Codigo_Item", "Valor_Lab"]
            
            df = lf.select(cols_ok).collect()

            if df.is_empty():
                st.warning("No hay datos para esta IPRESS.")
                return

            # Formateo
            df = df.with_columns([
                pl.col("Fecha_Atencion").cast(pl.Date),
                pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
                pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null("")
            ])

            # IDs para Pivot
            df = df.with_columns(
                pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
                .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
                .otherwise(pl.col("Codigo_Item")).alias("ID_Col")
            )

            # Lista de los 30 ítems
            items = ["99801_TA", "99801_1", "96150.01_VARONES", "96150.01_MUJERES", "99401", "Z019", "Z017", 
                     "99209.02", "99209.03", "99199.22", "96150.02", "96150.03", "99402.09", "99173", 
                     "99401.16", "99401.33", "86703.01", "86318.01", "99401.34", "D0150", "99402.03", 
                     "90688", "Z030", "99199.58", "87342", "88141.01", "84152", "82270", "Z128", "99401.12"]

            # Pivotado
            df_p = df.filter(pl.col("ID_Col").is_in(items)).pivot(
                values="Fecha_Atencion", index="Numero_Documento_Paciente", on="ID_Col", aggregate_function="first"
            )

            # Info Personal
            df_pers = df.select(["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
                                 "Apellido_Materno_Paciente", "Nombres_Paciente"]).unique()

            # Unión y Pandas
            res = df_pers.join(df_p, on="Numero_Documento_Paciente", how="left").to_pandas()
            
            for c in items:
                if c not in res.columns: res[c] = None

            res["Avance %"] = (res[items].notna().sum(axis=1) / 30 * 100).round(1)
            res = res.fillna("❌")

            st.header(f"Auditoría: {', '.join(sel_ipress)}")
            st.dataframe(res, use_container_width=True)

        except Exception as e:
            st.error(f"Error en el procesamiento: {e}")

# 3. LLAMADA CONTROLADA
try:
    ejecutar_aplicacion()
except Exception as e:
    st.error(f"Error fatal de inicio: {e}")
