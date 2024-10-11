import pandas as pd

class Extraer:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract_data(self):
        try:
            data = {
                        'film': pd.read_excel(self.file_path, sheet_name = 'film'),
                        'inventory': pd.read_excel(self.file_path, sheet_name = 'inventory'),
                        'rental': pd.read_excel(self.file_path, sheet_name = 'rental'),
                        'customer': pd.read_excel(self.file_path, sheet_name='customer'),
                        'store': pd.read_excel(self.file_path, sheet_name = 'rental')
                    }
            return data
            
        except Exception as ex:
            print(f'Error durante la extraccion: {ex}')
            return None
        