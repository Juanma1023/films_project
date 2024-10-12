from ETL.proceso_etl import ETLProcess

# Este bloque se ejecuta solo si el script se ejecuta directamente
if __name__ == "__main__":
    file_path = 'films_project\Data\Films_2.xlsx'  # Ruta al archivo Excel cargado
    etl = ETLProcess(file_path)
    # Ejecutar el proceso ETL y almacenar el resultado en output_file
    output_file = etl.run()