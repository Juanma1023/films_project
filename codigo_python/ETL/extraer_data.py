import pandas as pd

class Extraer:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract_data(self):
        try:
            data = {
                'df_film': pd.read_excel(self.file_path, sheet_name = 'film'),
                'df_inventory': pd.read_excel(self.file_path, sheet_name = 'inventory'),
                'df_rental': pd.read_excel(self.file_path, sheet_name = 'rental'),
                'df_customer': pd.read_excel(self.file_path, sheet_name='customer'),
                'df_store': pd.read_excel(self.file_path, sheet_name = 'store')
                }
            return data
            
        except Exception as ex:
            print(f'Error durante la extraccion: {ex}')
            return None