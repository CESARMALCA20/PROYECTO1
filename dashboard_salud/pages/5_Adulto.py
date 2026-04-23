import streamlit as st
import polars as pl
import pandas as pd
import os

# --- CONFIGURACIÓN DE PÁGINA ---
pd.set_option("styler.render.max_elements", 1000000) 
st.set_page_config(layout="wide", page_title="Auditoría Final - San Pablo")

# ─── 3. CARGA DE DATOS (CONFIGURACIÓN LOCAL Y NUBE) ──────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Si el script está en la carpeta 'pages', subimos un nivel para hallar 'data'
if os.path.basename(BASE_DIR) == "pages":
    BASE_DIR = os.path.dirname(BASE_DIR)

ARCHIVO_PARQUET = os.path.join(BASE_DIR, "data", "reporte.parquet")

@st.cache_data
def cargar_datos():
    if not os.path.exists(ARCHIVO_PARQUET):
        return None
    try:
        # Forzamos el uso de pyarrow, que es el estándar en servidores
        df = pl.read_parquet(ARCHIVO_PARQUET, use_pyarrow=True)
        
        df = df.rename({col: col.strip() for col in df.columns})
        return df
    except Exception as e:
        # Esto te dirá el error real en la pantalla si algo falla
        st.error(f"Error al cargar Parquet: {e}")
        return None

if df_raw is None:
    st.error(f"⚠️ No se encontró el archivo en: {ARCHIVO_PARQUET}")
    st.stop()

# --- CONFIGURACIÓN DE LOS 30 ÍTEMS ---
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

# --- SIDEBAR: FILTROS ---
st.sidebar.header("Opciones de Filtrado")

# 1. Reparación de columnas de tiempo (Para evitar ColumnNotFoundError)
df_raw = df_raw.with_columns([
    pl.col("Fecha_Atencion").cast(pl.Date),
    pl.col("Fecha_Atencion").dt.month().alias("Mes_Num"),
    pl.col("Fecha_Atencion").dt.strftime("%B").alias("Mes_Nombre")
])

# Traducción de meses a español
meses_es = {
    "January": "ENERO", "February": "FEBRERO", "March": "MARZO", "April": "ABRIL",
    "May": "MAYO", "June": "JUNIO", "July": "JULIO", "August": "AGOSTO",
    "September": "SETIEMBRE", "October": "OCTUBRE", "November": "NOVIEMBRE", "December": "DICIEMBRE"
}
df_raw = df_raw.with_columns(pl.col("Mes_Nombre").replace(meses_es))

# Filtro IPRESS
lista_ipress = sorted([str(i).strip() for i in df_raw["Nombre_Establecimiento"].unique().to_list()])
target_name = "SAN LUIS BAJO - GRANDE"
default_selection = [i for i in lista_ipress if target_name in i]
sel_ipress = st.sidebar.multiselect("🏥 IPRESS", options=lista_ipress, default=default_selection)

# Filtro Mes
df_meses_lista = df_raw.select(["Mes_Num", "Mes_Nombre"]).unique().sort("Mes_Num")
sel_mes = st.sidebar.multiselect("📅 Mes de Atención", options=df_meses_lista["Mes_Nombre"].to_list())

sel_dni = st.sidebar.text_input("🔍 Buscar por DNI")

# Aplicar filtros base
df_f = df_raw.with_columns(pl.col("Nombre_Establecimiento").str.strip_chars())

# Filtro de Edad Adulto (30-59)
df_f = df_f.filter((pl.col("Anio_Actual_Paciente") >= 30) & (pl.col("Anio_Actual_Paciente") <= 59))

if sel_ipress: df_f = df_f.filter(pl.col("Nombre_Establecimiento").is_in(sel_ipress))
if sel_mes: df_f = df_f.filter(pl.col("Mes_Nombre").is_in(sel_mes))
if sel_dni: df_f = df_f.filter(pl.col("Numero_Documento_Paciente").cast(pl.Utf8).str.contains(sel_dni))

# --- PROCESAMIENTO ---
if not df_f.is_empty():
    df_f = df_f.with_columns([
        pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars(),
        pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null(""),
        (pl.col("Apellido_Paterno_Personal") + " " + pl.col("Apellido_Materno_Personal") + " " + pl.col("Nombres_Personal")).alias("Profesional")
    ])

    codigos_lista = [c[0] for c in ITEMS_CONFIG]
    
    def asignar_columna_pivot(row):
        cod = row["Codigo_Item"]
        vlab = row["Valor_Lab"]
        if cod == "99801":
            return f"99801_{vlab}" if vlab in ["TA", "1"] else None
        if cod == "96150.01":
            return f"96150.01_{vlab}" if vlab in ["VARONES", "MUJERES"] else None
        return cod if cod in codigos_lista else None

    df_proc = df_f.with_columns([
        pl.struct(["Codigo_Item", "Valor_Lab"]).map_elements(asignar_columna_pivot, return_dtype=pl.Utf8).alias("ID_Columna"),
        pl.struct(["Fecha_Atencion", "Valor_Lab"]).map_elements(
            lambda x: f"{x['Fecha_Atencion'].strftime('%d/%m/%Y')} ({x['Valor_Lab']})" if x['Valor_Lab'] != "" else x['Fecha_Atencion'].strftime('%d/%m/%Y'),
            return_dtype=pl.Utf8
        ).alias("Contenido_Celda")
    ]).filter(pl.col("ID_Columna").is_not_null())

    if not df_proc.is_empty():
        df_pivot = df_proc.pivot(
            values="Contenido_Celda", index="Numero_Documento_Paciente", on="ID_Columna", aggregate_function="first"
        )

        cols_paciente = ["Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Nombre_Establecimiento", 
                         "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
                         "Nombres_Paciente", "Anio_Actual_Paciente", "Genero", "Descripcion_Financiador", "Profesional"]
        
        df_info = (
            df_proc.select(cols_paciente)
            .unique(subset=["Numero_Documento_Paciente"], keep="last")
            .with_columns(pl.col("Fecha_Atencion").dt.strftime("%d/%m/%Y"))
        )

        df_final = df_info.join(df_pivot, on="Numero_Documento_Paciente", how="left").to_pandas()

        ids_pivot = [c[1] for c in ITEMS_CONFIG]
        mapeo_visual = {c[1]: c[2] for c in ITEMS_CONFIG}
        
        for col_id in ids_pivot:
            if col_id not in df_final.columns: df_final[col_id] = None
        
        df_final = df_final.rename(columns=mapeo_visual)
        columnas_indicadores = list(mapeo_visual.values())
        
        df_final["Realizados"] = df_final[columnas_indicadores].notna().sum(axis=1)
        df_final["Faltan"] = 30 - df_final["Realizados"]
        df_final["Avance %"] = ((df_final["Realizados"] / 30) * 100).round(1)

        columnas_datos = ["Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Nombre_Establecimiento", 
                          "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", 
                          "Nombres_Paciente", "Anio_Actual_Paciente", "Genero", "Descripcion_Financiador", "Profesional"]
        
        nuevo_orden = columnas_datos + ["Realizados", "Faltan", "Avance %"] + columnas_indicadores
        df_final = df_final[nuevo_orden]
        df_final[columnas_indicadores] = df_final[columnas_indicadores].fillna("❌")

        df_final = df_final.sort_values(by="Avance %", ascending=False)
        df_final = df_final.reset_index(drop=True)
        df_final.index = df_final.index + 1

        st.markdown('<h2 style="color:#38bdf8;text-align:center;">SEGUIMIENTO DE PAQUETE ADULTO - RED SAN PABLO 2026</h2>', unsafe_allow_html=True)
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Pacientes Evaluados", len(df_final))
        m2.metric("Completos (100%)", len(df_final[df_final["Avance %"] >= 99.9]))
        m3.metric("Avance Promedio", f"{df_final['Avance %'].mean():.1f}%")

        st.dataframe(
            df_final.style.format({"Avance %": "{:.1f}"})
            .map(lambda x: 'color: #ff4b4b;' if x == "❌" else 'color: #28a745;', subset=columnas_indicadores)
            .map(lambda val: f'background-color: {"#28a745" if val >= 99.9 else "#f1c40f" if val >= 50 else "#ff4b4b"}; color: white; font-weight: bold', subset=["Avance %"]),
            use_container_width=True, height=650
        )
        
        csv = df_final.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Descargar Reporte en CSV", csv, "auditoria_san_pablo.csv", "text/csv")
    else:
        st.warning("No se encontraron registros de pacientes adultos.")
else:
    st.info("Ajuste los filtros laterales para visualizar los datos.")
