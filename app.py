import streamlit as st
import pandas as pd
import duckdb
import pyarrow.parquet as pq
import io


st.set_page_config(
    page_title="Parquet Viwer APP",
    page_icon= 'https://cdn.icon-icons.com/icons2/836/PNG/512/Amazon_icon-icons.com_66787.png',
    layout="centered",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.amazon.com',
        'Report a bug': "https://www.amazon.com",
        'About': """
            ## Parquet Viewer Tool
            Version 1.0.0  
            Developed by: amazon.com  
            
            This application allows you to upload and analyze Parquet files, convert lists to SQL format, and perform custom SQL queries.  
            
            © 2024 Amazon.com. All rights reserved.
        """
    }
)

# Configuración de la interfaz de usuario
st.title("Herramienta de Visualización y Consulta de Archivos Parquet")

# Cargar archivo Parquet
uploaded_file = st.file_uploader("Sube un archivo Parquet", type="parquet")
if uploaded_file:
    # Leer el archivo Parquet y mostrarlo en la aplicación
    data = pd.read_parquet(uploaded_file)
    
    # Mostrar información sobre el archivo
    num_rows, num_columns = data.shape
    st.write(f"El archivo Parquet contiene **{num_rows} filas** y **{num_columns} columnas**.")
    
    st.write("Vista previa de los datos cargados:")
    st.write(data.head())

    # Configurar DuckDB para consultas SQL
    conn = duckdb.connect(database=':memory:')  # Conexión a una base de datos en memoria
    conn.register('data', data)  # Registrar el DataFrame en DuckDB

    # Consulta SQL personalizada
    query = st.text_area("Escribe tu consulta SQL:", "SELECT * FROM data WHERE partid IN ('Inserte_ASIN')")
    if st.button("Ejecutar consulta"):
        try:
            result = conn.execute(query).fetchdf()  # Ejecuta la consulta y obtiene los resultados como DataFrame
            st.write("Resultados de la consulta:")
            st.write(result)
            
            # Exportar a CSV
            csv = result.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar resultados como CSV",
                data=csv,
                file_name="resultados.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Error en la consulta SQL: {e}")

# Funcionalidad de conversión de lista a formato SQL
st.write("### Convertidor de Lista a Formato SQL")
user_input = st.text_area("Pega tu lista aquí (una entrada por línea):")

# Convertir el texto pegado al formato SQL si hay texto en el cuadro
if user_input:
    # Dividir las líneas, limpiar espacios y envolver cada entrada en comillas simples sin espacios
    items = [f"'{item.strip()}'" for item in user_input.splitlines() if item.strip()]
    
    # Unir los elementos con una coma sin espacio
    formatted_output = ",".join(items)
    
    # Mostrar el resultado formateado
    st.write("Formato SQL:")
    st.code(formatted_output, language='sql')
    
    # Botón para descargar el formato SQL
    st.download_button(
        label="Descargar formato SQL",
        data=formatted_output,
        file_name="formatted_sql.txt",
        mime="text/plain"
    )
