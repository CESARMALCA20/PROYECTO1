import streamlit as st
import polars as pl
import pandas as pd
import os

# --- CONFIGURACIÓN DE PÁGINA ---
pd.set_option("styler.render.max_elements", 1000000) 
st.set_page_config(layout="wide", page_title="Auditoría Adulto - San Pablo")

# ─── 1. CARGA DE DATOS ROBUSTA ───────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)

ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

@st.cache_data
def cargar_datos_seguros():
    if not os.path.exists(ARCHIVO_PARQUET):
        return None
    try:
        # Forzamos pyarrow para Linux (Streamlit Cloud)
        df = pl.read_parquet(ARCHIVO_PARQUET, use_pyarrow=True)
        df = df.rename({col: col.strip() for col in df.columns})
        
        # PROCESAMOS FECHAS AQUÍ MISMO para que el sidebar no falle
        df = df.with_columns([
            pl.col("Fecha_Atencion").cast(pl.Date),
            pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"),
            pl.col("Fecha_Atencion").dt.strftime("%B").alias("Mes_Nombre")
        ])
        
        # Traducción inmediata
        meses_es = {
            "January": "ENERO", "February": "FEBRERO", "March": "MARZO", "April": "ABRIL",
            "May": "MAYO", "June": "JUNIO", "July": "JULIO", "August": "AGOSTO",
            "September": "SETIEMBRE", "October": "OCTUBRE", "November": "NOVIEMBRE", "December": "DICIEMBRE"
        }
        df = df.with_columns(pl.col("Mes_Nombre").replace(meses_es))
        
        return df
    except Exception as e:
        return None

df_raw = cargar_datos_seguros()

# Si falla la carga, mostramos error y detenemos antes de que intente hacer filtros
if df_raw is None:
    st.error("⚠️ Error: No se pudo cargar la base de datos. Verifica que 'data/reporte.parquet' exista en GitHub.")
    st.stop()

# ─── 2. FILTROS (SIDEBAR) ────────────────────────────────────────────────────
st.sidebar.header("Opciones de Filtrado")

# Filtro IPRESS - Buscamos la IPRESS por defecto de forma segura
lista_ipress = sorted(df_raw["Nombre_Establecimiento"].unique().to_list())
target_name = "SAN LUIS BAJO - GRANDE"
default_sel = [i for i in lista_ipress if target_name in i]

sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress, default=default_sel)

# Filtro Mes - Ya tenemos Mes_Nombre porque lo creamos en cargar_datos_seguros
df_meses_lista = df_raw.select(["Mes_Num", "Mes_Nombre"]).unique().sort("Mes_Num")
sel_mes = st.sidebar.multiselect("📅 Mes de Atención", options=df_meses_lista["Mes_Nombre"].to_list())

sel_dni = st.sidebar.text_input("🔍 Buscar por DNI")

# ─── 3. PROCESAMIENTO DE DATOS ───────────────────────────────────────────────
# Filtrar por edad Adulto
df_f = df_raw.filter((pl.col("Anio_Actual_Paciente") >= 30) & (pl.col("Anio_Actual_Paciente") <= 59))

if sel_ipress:
    df_f = df_f.filter(pl.col("Nombre_Establecimiento").is_in(sel_ipress))
if sel_mes:
    df_f = df_f.filter(pl.col("Mes_Nombre").is_in(sel_mes))
if sel_dni:
    df_f = df_f.filter(pl.col("Numero_Documento_Paciente").cast(pl.Utf8).str.contains(sel_dni))

# CONFIGURACIÓN DE LOS 30 ÍTEMS (ID, Columna_Destino, Titulo_Visual)
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
    # Pivotado y limpieza
    df_f = df_f.with_columns([
        pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
        pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
        (pl.col("Apellido_Paterno_Personal") + " " + pl.col("Apellido_Materno_Personal") + " " + pl.col("Nombres_Personal")).alias("Profesional")
    ])

    codigos_validos = [c[0] for c in ITEMS_CONFIG]
    
    def asignar_id(row):
        cod, lab = row["Codigo_Item"], row["Valor_Lab"]
        if cod == "99801": return f"99801_{lab}" if lab in ["TA", "1"] else None
        if cod == "96150.01": return f"96150.01_{lab}" if lab in ["VARONES", "MUJERES"] else None
        return cod if cod in codigos_validos else None

    df_proc = df_f.with_columns([
        pl.struct(["Codigo_Item", "Valor_Lab"]).map_elements(asignar_id, return_dtype=pl.Utf8).alias("ID_Columna"),
        pl.struct(["Fecha_Atencion", "Valor_Lab"]).map_elements(
            lambda x: f"{x['Fecha_Atencion'].strftime('%d/%m/%Y')} ({x['Valor_Lab']})" if x['Valor_Lab'] else x['Fecha_Atencion'].strftime('%d/%m/%Y'),
            return_dtype=pl.Utf8
        ).alias("Contenido")
    ]).filter(pl.col("ID_Columna").is_not_null())

    if not df_proc.is_empty():
        df_piv = df_proc.pivot(values="Contenido", index="Numero_Documento_Paciente", on="ID_Columna", aggregate_function="first")
        
        df_info = df_proc.select(["Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Nombre_Establecimiento", 
                                  "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
                                  "Nombres_Paciente", "Anio_Actual_Paciente", "Genero", "Descripcion_Financiador", "Profesional"]).unique(subset=["Numero_Documento_Paciente"])
        
        df_final = df_info.join(df_piv, on="Numero_Documento_Paciente", how="left").to_pandas()

        # Asegurar todas las columnas y renombrar
        for _, col_id, titulo in ITEMS_CONFIG:
            if col_id not in df_final.columns: df_final[col_id] = None
            df_final = df_final.rename(columns={col_id: titulo})

        titulos = [c[2] for c in ITEMS_CONFIG]
        df_final["Realizados"] = df_final[titulos].notna().sum(axis=1)
        df_final["Avance %"] = ((df_final["Realizados"] / 30) * 100).round(1)
        df_final[titulos] = df_final[titulos].fillna("❌")

        # --- INTERFAZ ---
        st.markdown(f'<h2 style="color:#38bdf8;text-align:center;">PAQUETE ADULTO: {", ".join(sel_ipress) if sel_ipress else "TODOS"}</h2>', unsafe_allow_html=True)
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Pacientes", len(df_final))
        m2.metric("Avance Promedio", f"{df_final['Avance %'].mean():.1f}%")
        m3.metric("Filtro Mes", ", ".join(sel_mes) if sel_mes else "Todos")

        st.dataframe(df_final, use_container_width=True, height=600)
    else:
        st.warning("No hay datos para los filtros seleccionados.")
else:
    st.info("Seleccione filtros en el sidebar para comenzar.")
