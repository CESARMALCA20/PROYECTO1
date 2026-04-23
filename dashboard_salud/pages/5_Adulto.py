import streamlit as st
import polars as pl
import pandas as pd
import os
from pathlib import Path

# --- CONFIGURACIÓN DE PÁGINA ---
pd.set_option("styler.render.max_elements", 1000000) 
st.set_page_config(layout="wide", page_title="Auditoría Final - San Pablo")

# ─── 3. CARGA DE DATOS FRAGMENTADA (OPTIMIZADO PARA LA NUBE) ──────────────────
# Localizamos la carpeta db_split que subiste a GitHub
BASE_DIR = Path(__file__).resolve().parent.parent
DB_SPLIT_DIR = BASE_DIR / "data" / "db_split"

# 1. Obtener lista de archivos (Esto no consume RAM)
if not DB_SPLIT_DIR.exists():
    st.error(f"⚠️ No se encuentra la carpeta: {DB_SPLIT_DIR}. Asegúrate de subir 'db_split' a GitHub dentro de la carpeta 'data'.")
    st.stop()

# Mapeamos los archivos para el selector
archivos_parquet = sorted([f.name for f in DB_SPLIT_DIR.glob("*.parquet")])
nombres_visibles = [f.replace(".parquet", "").replace("_", " ") for f in archivos_parquet]
diccionario_ipress = dict(zip(nombres_visibles, archivos_parquet))

# --- SIDEBAR: FILTROS ---
st.sidebar.header("Opciones de Filtrado")

# Usamos selectbox para cargar solo UNA IPRESS a la vez (vital para evitar crash)
target_default = "SAN LUIS BAJO - GRANDE"
default_idx = 0
if any(target_default in n.upper() for n in nombres_visibles):
    # Buscamos el índice del valor por defecto si existe
    for i, nombre in enumerate(nombres_visibles):
        if target_default in nombre.upper():
            default_idx = i
            break

sel_ipress_name = st.sidebar.selectbox("🏥 Seleccione IPRESS", options=nombres_visibles, index=default_idx)

@st.cache_data
def cargar_fragmento(nombre_ipress):
    archivo = diccionario_ipress[nombre_ipress]
    ruta = DB_SPLIT_DIR / archivo
    # Cargamos solo el fragmento pequeño (1MB - 5MB aprox)
    df = pl.read_parquet(str(ruta))
    # Limpieza de nombres de columnas que ya tenías
    df = df.rename({col: col.strip() for col in df.columns})
    return df

# Este es el DataFrame que alimentará el resto de tu código
df_raw = cargar_fragmento(sel_ipress_name)

# --- CONFIGURACIÓN DE LOS 30 ÍTEMS (TU LÓGICA ORIGINAL) ---
items_config = [
    {"ID": "99801", "Valor": "TA", "Nombre": "Presión Arterial"},
    {"ID": "99801", "Valor": "1", "Nombre": "Examen de Mamas"},
    {"ID": "96150.01", "Valor": "VARONES", "Nombre": "Cuestionario Violencia (V)"},
    {"ID": "96150.01", "Valor": "MUJERES", "Nombre": "Cuestionario Violencia (M)"},
    {"ID": "99401", "Valor": None, "Nombre": "Consejería Estilos de Vida"},
    {"ID": "Z019", "Valor": None, "Nombre": "Evaluación General (Agudeza Visual)"},
    {"ID": "Z017", "Valor": None, "Nombre": "Evaluación General (Agudeza Auditiva)"},
    {"ID": "99209.02", "Valor": None, "Nombre": "Evaluación Clínica Preventiva (1)"},
    {"ID": "99209.03", "Valor": None, "Nombre": "Evaluación Clínica Preventiva (2)"},
    {"ID": "99199.22", "Valor": None, "Nombre": "Tamizaje Salud Mental (SRQ/ASSIST)"},
    {"ID": "96150.02", "Valor": None, "Nombre": "Problemas Psicosociales (1)"},
    {"ID": "96150.03", "Valor": None, "Nombre": "Problemas Psicosociales (2)"},
    {"ID": "99402.09", "Nombre": "Consejería Salud Mental"},
    {"ID": "99173", "Nombre": "Tamizaje de Agudeza Visual"},
    {"ID": "99401.16", "Nombre": "Consejería Prevención de Cáncer"},
    {"ID": "99401.33", "Nombre": "Consejería ITS/VIH/SÍFILIS"},
    {"ID": "86703.01", "Nombre": "Tamizaje VIH (Prueba Rápida)"},
    {"ID": "86318.01", "Nombre": "Tamizaje Sífilis (Prueba Rápida)"},
    {"ID": "99401.34", "Nombre": "Consejería Prevención TBC"},
    {"ID": "D0150", "Nombre": "Examen Dental"},
    {"ID": "99402.03", "Nombre": "Consejería Salud Bucal"},
    {"ID": "90688", "Nombre": "Vacuna Influenza"},
    {"ID": "Z030", "Nombre": "Vacuna DT"},
    {"ID": "99199.58", "Nombre": "Vacuna Hepatitis B"},
    {"ID": "87342", "Nombre": "Tamizaje Hepatitis B"},
    {"ID": "88141.01", "Nombre": "Papanicolaou / IVAA"},
    {"ID": "84152", "Nombre": "Tamizaje PSA (Cáncer Próstata)"},
    {"ID": "82270", "Nombre": "Tamizaje Colon/Recto (Thevenon)"},
    {"ID": "Z128", "Nombre": "Tamizaje de Cáncer de Piel"},
    {"ID": "99401.12", "Nombre": "Consejería Nutricional"}
]

# --- FILTROS DE MES Y EDAD ---
meses_dict = {
    "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6,
    "Julio": 7, "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
}
sel_meses = st.sidebar.multiselect("📅 Meses de Atención", options=list(meses_dict.keys()), default=list(meses_dict.keys()))
meses_numeros = [meses_dict[m] for m in sel_meses]

# Filtro de edad obligatorio para el paquete Adulto
df_f = df_raw.filter(
    (pl.col("Anio_Actual_Paciente") >= 30) & 
    (pl.col("Anio_Actual_Paciente") <= 59) &
    (pl.col("Fecha_Atencion").dt.month().is_in(meses_numeros))
)

# ─── 4. PROCESAMIENTO (TU LÓGICA DE AUDITORÍA) ──────────────────────────────
if not df_f.is_empty():
    with st.spinner("Generando Auditoría..."):
        # Limpieza de códigos e IDs
        df_f = df_f.with_columns([
            pl.col("Codigo_Item").cast(pl.Utf8).str.strip_chars().alias("Codigo_Item"),
            pl.col("Valor_Lab").cast(pl.Utf8).str.strip_chars().fill_null("").alias("Valor_Lab")
        ])

        # Crear columna de cruce ID_VALOR
        df_f = df_f.with_columns(
            pl.when(pl.col("Codigo_Item") == "99801").then(pl.format("99801_{}", pl.col("Valor_Lab")))
            .when(pl.col("Codigo_Item") == "96150.01").then(pl.format("96150.01_{}", pl.col("Valor_Lab")))
            .otherwise(pl.col("Codigo_Item")).alias("ID_AUDITORIA")
        )

        columnas_indicadores = []
        for item in items_config:
            cod = f"{item['ID']}_{item['Valor']}" if item.get("Valor") else item["ID"]
            columnas_indicadores.append(cod)

        # Pivotado
        df_pivot = df_f.filter(pl.col("ID_AUDITORIA").is_in(columnas_indicadores)).pivot(
            values="Fecha_Atencion",
            index="Numero_Documento_Paciente",
            on="ID_AUDITORIA",
            aggregate_function="first"
        )

        # Datos del Paciente
        df_pacientes = df_f.select([
            "Numero_Documento_Paciente", "Apellido_Paterno_Paciente", 
            "Apellido_Materno_Paciente", "Nombres_Paciente", "Anio_Actual_Paciente"
        ]).unique()

        df_final_pl = df_pacientes.join(df_pivot, on="Numero_Documento_Paciente", how="left")
        
        # Pasar a Pandas para visualización final
        df_final = df_final_pl.to_pandas()

        for col in columnas_indicadores:
            if col not in df_final.columns:
                df_final[col] = None

        # Cálculos de Avance
        df_final["Completos"] = df_final[columnas_indicadores].notna().sum(axis=1)
        df_final["Faltan"] = len(columnas_indicadores) - df_final["Completos"]
        df_final["Avance %"] = (df_final["Completos"] / len(columnas_indicadores)) * 100

        # Ordenar y Limpiar
        nuevo_orden = ["Numero_Documento_Paciente", "Apellido_Paterno_Paciente", "Apellido_Materno_Paciente", "Nombres_Paciente", "Anio_Actual_Paciente", "Completos", "Faltan", "Avance %"] + columnas_indicadores
        df_final = df_final[nuevo_orden]
        df_final[columnas_indicadores] = df_final[columnas_indicadores].fillna("❌")

        df_final = df_final.sort_values(by="Avance %", ascending=False)
        df_final = df_final.reset_index(drop=True)
        df_final.index = df_final.index + 1

        # --- VISUALIZACIÓN ---
        st.markdown('<h2 style="color:#38bdf8;text-align:center;">SEGUIMIENTO DE PAQUETE ADULTO - RED SAN PABLO 2026</h2>', unsafe_allow_html=True)
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Pacientes Evaluados", len(df_final))
        m2.metric("Completos (100%)", len(df_final[df_final["Avance %"] >= 99.9]))
        m3.metric("Avance Promedio", f"{df_final['Avance %'].mean():.1f}%")

        st.dataframe(
            df_final.style.format({"Avance %": "{:.1f}"})
            .map(lambda x: 'color: #ff4b4b;' if x == "❌" else 'color: #28a745;', subset=columnas_indicadores)
            .map(lambda val: f'background-color: {"#28a745" if val >= 99.9 else "#f1c40f" if val >= 50 else "#ff4b4b"}; color: white;', subset=["Avance %"]),
            use_container_width=True, height=600
        )
else:
    st.warning(f"No hay registros encontrados para {sel_ipress_name} en los meses seleccionados.")
