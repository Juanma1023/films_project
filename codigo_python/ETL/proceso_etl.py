from ETL.extraer_data import Extraer

class ETLProcess:
    def __init__(self, file_path):
        self.file_path = file_path
        self.extractor = Extraer(self.file_path)

    def run(self):
        # Extracción de datos
        data = self.extractor.extract_data()
        return data