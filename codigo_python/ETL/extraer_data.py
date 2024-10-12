import pandas as pd

class Extraer:
    def __init__(self, file_path):
        """
        Inicializa la clase Extraer con la ruta del archivo.
        :param file_path: Ruta al archivo Excel que se va a procesar.
        """
        self.file_path = file_path

    def extract_data(self):
        """
        Extrae datos de un archivo Excel y los convierte en un diccionario.
        :return: Un diccionario que contiene los datos de todas las hojas del Excel en forma de registros,
        o None si ocurre un error durante la extracción.
        """
        try:
            # Leer todas las hojas del archivo Excel y almacenarlas en un diccionario
            excel_data = pd.read_excel(self.file_path, sheet_name=None)
            # Convertir cada hoja en un diccionario de registros (lista de diccionarios)
            sheets_data = {sheet_name: sheet_df.to_dict(orient='records') for sheet_name, sheet_df in excel_data.items()}
            return sheets_data
            
        except Exception as ex:
            print(f'Error durante la extracción: {ex}')
            return None
