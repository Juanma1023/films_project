import pandas as pd

class Load:
    def __init__(self, diccionario, nombre_archivo):
        self.diccionario = diccionario
        self.nombre_archivo = nombre_archivo

    def guardar_diccionario_en_excel(self):
        # Crear un escritor de Excel
        with pd.ExcelWriter(self.nombre_archivo, engine='openpyxl') as writer:
            # Recorrer el diccionario
            for nombre_hoja, filas in self.diccionario.items():
                # Convertir la lista de diccionarios en un DataFrame de pandas
                df = pd.DataFrame(filas)
                # Guardar cada DataFrame en una hoja diferente
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)