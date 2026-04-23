import streamlit as st
import polars as pl
import pandas as pd
from pathlib import Path
import os

# Intentar forzar la carga del motor parquet
try:
    import pyarrow
except ImportError:
    st.error("Falta la librería 'pyarrow'. Agrégala a requirements.txt")
    st.stop()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(layout="wide", page_title="Auditoría Adulto")

# ─── 1. LOCALIZACIÓN DEL ARCHIVO (MÉTODO ROBUSTO) ──────────────────────────
# Obtenemos la ruta de este archivo: proyecto/pages/5_Adulto.py
current_path = Path(__file__).resolve()
# Subimos dos niveles para llegar a la raíz: proyecto/
root_path = current_path.parent.parent
# Ruta final: proyecto/data/reporte.parquet
ARCHIVO_PARQUET = root_path / "data" / "reporte.parquet"

@st.cache_data
def cargar_datos_seguros():
    if not ARCHIVO_PARQUET.exists():
        return None
    try:
        # Leemos especificando el motor pyarrow
        df = pl.read_parquet(str(ARCHIVO_PARQUET), use_pyarrow=True)
        
        # Limpieza de columnas
        df = df.rename({col: col.strip() for col in df.columns})
        
        # Creación de columnas de tiempo INMEDIATA
        df = df.with_columns([
            pl.col("Fecha_Atencion").cast(pl.Date),
            pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"),
            pl.col("Fecha_Atencion").dt.strftime("%B").alias("Mes_Nombre")
        ])
        
        # Traducción
        meses_dict = {
            "January": "ENERO", "February": "FEBRERO", "March": "MARZO", "April": "ABRIL",
            "May": "MAYO", "June": "JUNIO", "July": "JULIO", "August": "AGOSTO",
            "September": "SETIEMBRE", "October": "OCTUBRE", "November": "NOVIEMBRE", "December": "DICIEMBRE"
        }
        df = df.with_columns(pl.col("Mes_Nombre").replace(meses_dict))
        return df
    except Exception as e:
        st.error(f"⚠️ Error Crítico: {str(e)}")
        return None

# Ejecución de carga
try:
    df_raw = cargar_datos_seguros()
except Exception as e:
    st.error(f"Error al ejecutar cargar_datos_seguros: {e}")
    st.stop()

if df_raw is None:
    st.error(f"❌ ARCHIVO NO ENCONTRADO EN: {ARCHIVO_PARQUET}")
    st.info("Asegúrate de que la carpeta 'data' y el archivo 'reporte.parquet' estén subidos a GitHub.")
    st.stop()

# ─── 2. FILTROS (SIDEBAR) ──────────────────────────────────────────────────
st.sidebar.header("Filtros de Auditoría")

lista_ipress = sorted(df_raw["Nombre_Establecimiento"].unique().to_list())
# Buscamos San Luis Bajo Grande de forma que no importe si hay espacios de más
target = "SAN LUIS BAJO - GRANDE"
default_val = [i for i in lista_ipress if target.upper() in i.upper()]

sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress, default=default_val)

meses_disp = df_raw.select(["Mes_Num", "Mes_Nombre"]).unique().sort("Mes_Num")
sel_mes = st.sidebar.multiselect("📅 Mes", options=meses_disp["Mes_Nombre"].to_list())

# ─── 3. PROCESAMIENTO ──────────────────────────────────────────────────────
# Filtro por Edad
df_f = df_raw.filter((pl.col("Anio_Actual_Paciente") >= 30) & (pl.col("Anio_Actual_Paciente") <= 59))

if sel_ipress:
    df_f = df_f.filter(pl.col("Nombre_Establecimiento").is_in(sel_ipress))
if sel_mes:
    df_f = df_f.filter(pl.col("Mes_Nombre").is_in(sel_mes))

# CONFIGURACIÓN DE LOS 30 ÍTEMS
ITEMS_CONFIG = [
    ("99801", "99801_TA", "99801\nPLAN ELABORADO"), ("99801", "99801_1", "99801\nPLAN EJECUTADO"),
    ("96150.01", "96150.01_VARONES", "96150.01\nVIF (VARONES)"), ("96150.01", "96150.01_MUJERES", "96150.01\nVIF (MUJERES)"),
    ("99401", "99401", "99401\nCONSEJERIA INT."), ("Z019", "Z019", "Z019\nVALORACIÓN RIESGO"),
    ("Z017", "Z017", "Z017\nEXAM. LABORATORIO"), ("99209.02", "99209.02", "99209.02\nIMC"),
    ("99209.03", "99209.03", "99209.03\nPERIMETRO ABD."), ("99199.22", "99199.22", "99199.22\nPRESION ART."),
    ("96150.02", "96150.02", "96150.02\nALCOHOL/DROGAS"), ("96150.03", "96150.03", "96150.03\nDEPRESIÓN PHQ-9"),
    ("99402.09", "99402.09", "99402.09\nCONS. S. MENTAL"), ("99173", "99173", "99173\nAGUDEZA VISUAL"),
    ("99401.16", "99401.16", "99401.16\nCONS. S. OCULAR"), ("99401.33", "99401.33", "99401.33\nPRE-TEST VIH"),
    ("86703.01", "86703.01", "86703.01\nDETECCION VIH"), ("86318.01", "86318.01", "86318.01\nVIH RÁPIDA"),
    ("99401.34", "99401.34", "99401.34\nPOST-TEST VIH"), ("D0150", "D0150", "D0150\nEXAMEN ORAL"),
    ("99402.03", "99402.03", "99402.03\nCONS. SSYRR"), ("90688", "90688", "90688\nVAC. INFLUENZA"),
    ("Z030", "Z030", "Z030\nTAMIZAJE TB"), ("99199.58", "99199.58", "99199.58\nGLUCOSA"),
    ("87342", "87342", "87342\nHEPATITIS B"), ("88141.01", "88141.01", "88141.01\nCANCER UTERO"),
    ("84152", "84152", "84152\nCANCER PROSTATA"), ("82270", "82270", "82270\nCANCER COLON"),
    ("Z128", "Z128", "Z128\nCANCER PIEL"), ("99401.12", "99401.12", "99401.12\nESTILOS VIDA")
]

if not df_f.is_empty():
    # Pivotado
    df_f = df_f.with_columns([
        pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
        pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
        (pl.col("Apellido_Paterno_Personal") + " " + pl.col("Apellido_Materno_Personal") + " " + pl.col("Nombres_Personal")).alias("Profesional")
    ])

    codes = [c[0] for c in ITEMS_CONFIG]
    
    def get_id(row):
        c, l = row["Codigo_Item"], row["Valor_Lab"]
        if c == "99801": return f"99801_{l}" if l in ["TA", "1"] else None
        if c == "96150.01": return f"96150.01_{l}" if l in ["VARONES", "MUJERES"] else None
        return c if c in codes else None

    df_proc = df_f.with_columns([
        pl.struct(["Codigo_Item", "Valor_Lab"]).map_elements(get_id, return_dtype=pl.Utf8).alias("ID_Col"),
        pl.struct(["Fecha_Atencion", "Valor_Lab"]).map_elements(
            lambda x: f"{x['Fecha_Atencion'].strftime('%d/%m/%Y')} ({x['Valor_Lab']})" if x['Valor_Lab'] else x['Fecha_Atencion'].strftime('%d/%m/%Y'),
            return_dtype=pl.Utf8
        ).alias("Txt")
    ]).filter(pl.col("ID_Col").is_not_null())

    if not df_proc.is_empty():
        df_piv = df_proc.pivot(values="Txt", index="Numero_Documento_Paciente", on="ID_Col", aggregate_function="first")
        
        # Unir info de paciente
        cols_info = ["Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Nombre_Establecimiento", 
                     "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
                     "Nombres_Paciente", "Anio_Actual_Paciente", "Genero", "Descripcion_Financiador", "Profesional"]
        
        df_base = df_proc.select(cols_info).unique(subset=["Numero_Documento_Paciente"])
        df_final = df_base.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()

        # Renombrar y rellenar
        titulos_visibles = []
        for _, cid, t in ITEMS_CONFIG:
            if cid not in df_final.columns: df_final[cid] = None
            df_final = df_final.rename(columns={cid: t})
            titulos_visibles.append(t)

        df_final["Avance %"] = (df_final[titulos_visibles].notna().sum(axis=1) / 30 * 100).round(1)
        df_final[titulos_visibles] = df_final[titulos_visibles].fillna("❌")

        # --- MOSTRAR ---
        st.header("📊 Seguimiento Paquete Adulto")
        st.write(f"Mostrando data de: {', '.join(sel_ipress) if sel_ipress else 'Todas las IPRESS'}")
        
        st.dataframe(df_final, use_container_width=True)
    else:
        st.warning("No hay datos para estos filtros.")
else:
    st.info("Ajuste los filtros para ver la información.")
