from .extraer_data import Extraer
from .transformar_data import Transformar
from .cargar import Load

class ETLProcess:
    def __init__(self, file_path):
        self.file_path = file_path
        self.extractor = Extraer(self.file_path)

    def run(self):
        # Extracción de datos utilizando el método de la clase Extraer
        data = self.extractor.extract_data()

        # Diccionario que mapea las claves de las listas a sus respectivos IDs
        keys_ids = {
            "store": "store_id",
            "film": "film_id",
            "inventory": "inventory_id",
            "customer": "customer_id",
            "rental": "rental_id"
        }

        # Ciclo para eliminar diccionarios repetidos para cada par de clave e ID
        for key, id_key in keys_ids.items():    
            # Eliminar diccionarios repetidos
            data = self.eliminar_diccionarios_repetidos_general(data, key, id_key)

        # Limpiar los espacios en las claves de los diccionarios
        for key, value in keys_ids.items():
            data = self.limpiar_espacios_claves(data, key)

        # Diccionario para almacenar los datos transformados
        diccionario = {}
        
        # Transformar los datos para cada clave
        for key, value in keys_ids.items():
            diccionario[key] = Transformar(key, data[key]).transformar_datos()

        # Guardar el diccionario transformado en un archivo Excel
        return Load(diccionario, 'films_project\Data\Films_2_limpia.xlsx').guardar_diccionario_en_excel()
    
    def limpiar_espacios_claves(self, diccionario_principal, nombre_lista):
        """
        Elimina los espacios al inicio y al final de las claves de los diccionarios en una lista.

        :param diccionario_principal: Diccionario que contiene la lista de diccionarios a procesar.
        :param nombre_lista: Nombre de la lista dentro del diccionario principal que se va a limpiar.
        :return: Diccionario principal con claves sin espacios.
        """
        # Obtener la lista de diccionarios del diccionario principal
        lista_diccionarios = diccionario_principal.get(nombre_lista, [])
        lista_limpia = []
        
        for diccionario in lista_diccionarios:
            # Crear un nuevo diccionario con las claves sin espacios
            diccionario_limpio = {clave.strip(): valor for clave, valor in diccionario.items()}
            lista_limpia.append(diccionario_limpio)
        
        # Actualizar el diccionario principal con la lista limpia
        diccionario_principal[nombre_lista] = lista_limpia
        return diccionario_principal

    def eliminar_diccionarios_repetidos_general(self, diccionario_principal, nombre_lista, clave):
        """
        Elimina diccionarios repetidos de una lista dentro del diccionario principal
        usando una clave específica como identificador.

        :param diccionario_principal: Diccionario que contiene la lista a procesar.
        :param nombre_lista: Nombre de la lista dentro del diccionario principal.
        :param clave: Clave utilizada para identificar duplicados.
        :return: Diccionario principal con la lista de diccionarios filtrada.
        """
        # Acceder a la lista de diccionarios según el nombre de la lista proporcionada
        lista_diccionarios = diccionario_principal.get(nombre_lista, [])
        
        # Crear una lista filtrada sin diccionarios repetidos
        lista_filtrada = []
        valores_vistos = set()

        for item in lista_diccionarios:
            # Usar la clave especificada como identificador para eliminar duplicados
            valor = item.get(clave)
            if valor not in valores_vistos:
                valores_vistos.add(valor)
                lista_filtrada.append(item)

        # Actualizar el diccionario principal con la lista filtrada
        diccionario_principal[nombre_lista] = lista_filtrada
        return diccionario_principal
