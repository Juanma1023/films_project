from ETL.proceso_etl import ETLProcess

if __name__ == "__main__":
    file_path = 'films_project\Data\Films_2.xlsx'  # Ruta al archivo Excel cargado
    etl = ETLProcess(file_path)
    etl.run()