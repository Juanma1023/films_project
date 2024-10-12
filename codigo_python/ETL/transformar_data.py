from pyspark.sql import SparkSession

class Transformar:
    def __init__(self, name, df_hoja):
        # Inicializa la clase con el nombre y el DataFrame
        self.spark = SparkSession.builder \
            .appName("ETL Films") \
            .getOrCreate()
        self.df_hoja = df_hoja
        self.name = name

    def validar_secuencia_consecutiva(self, diccionarios, clave, inicio):
        # Valida que los valores de una clave sean secuenciales comenzando desde un valor inicial
        secuencia_esperada = inicio

        for diccionario in diccionarios:
            valor_actual = diccionario.get(clave)
            # Si el valor actual no es igual a la secuencia esperada, lo corregimos
            if valor_actual != secuencia_esperada:
                diccionario[clave] = secuencia_esperada
            
            # Incrementamos la secuencia esperada para el siguiente elemento
            secuencia_esperada += 1
        
        return diccionarios

    def limpiarCampoNumerico(self, cadena):
        # Limpia la cadena para dejar solo caracteres numéricos
        numeros = ''.join(caracter for caracter in cadena if caracter.isdigit())
        return numeros
    
    def limpiarCampoFecha(self, cadena):
        # Limpia la cadena para dejar solo caracteres válidos para una fecha
        caracteres_validos = []
        
        for caracter in cadena:
            # Agregar dígitos y separadores válidos
            if caracter.isdigit() or caracter in ['-', ':', ' ']:
                caracteres_validos.append(caracter)

        # Unir la lista de caracteres en una cadena
        return ''.join(caracteres_validos)
    
    def limpiarCampoNumericoDecimal(self, cadena):
        # Limpia la cadena para dejar solo caracteres numéricos y un solo punto decimal
        encontrado_punto = False
        numeros = []
        
        for caracter in cadena:
            # Si es un dígito, lo agregamos
            if caracter.isdigit():
                numeros.append(caracter)
            # Si es un punto y aún no hemos encontrado otro, lo agregamos
            elif caracter == '.' and not encontrado_punto:
                numeros.append(caracter)
                encontrado_punto = True
        
        # Unir la lista de caracteres en una cadena
        return ''.join(numeros)
    
    def limpiarCorreo(self, correo):
        # Limpia el correo, convirtiendo la parte local a mayúsculas y cambiando el dominio
        if '@' in correo:
            parte_local = correo.split('@')[0] # Limitar a una división
            parte_local_mayuscula = parte_local.upper().replace("\\","") # Convertir a mayúsculas
            correo_limpio = parte_local_mayuscula + '@sakilacustomer.org'
            return correo_limpio
        else:
            return correo 
        
    def textoMayusculas(self, texto):
        # Convierte un texto a mayúsculas y reemplaza caracteres específicos
        texto_mayusculas = texto.upper().replace("\\", "'")
        return texto_mayusculas

    def transformar_datos(self):
        # Realiza la transformación de datos según el tipo de entidad
        if self.name == 'store':
            for diccionario in self.df_hoja:
                # Recorrer cada diccionario en el DataFrame
                for clave, valor in diccionario.items():
                    if 'store_id' == clave or 'manager_staff_id' == clave or 'address_id' == clave:
                        # Limpiar campos numéricos
                        diccionario[clave] = self.limpiarCampoNumerico(str(valor))
                    if 'last_update' == clave:
                        # Limpiar campos de fecha
                        diccionario[clave] = self.limpiarCampoFecha(str(valor))

        if self.name == 'film':
            for diccionario in self.df_hoja:
                for clave, valor in diccionario.items():
                    if 'film_id' == clave or 'release_year' == clave or 'language_id' == clave or 'rental_duration' == clave or 'length' == clave or 'num_voted_users' == clave:
                        # Limpiar campos numéricos
                        diccionario[clave] = self.limpiarCampoNumerico(str(valor))

                    if 'rental_rate' == clave or 'replacement_cost' == clave:
                        # Limpiar campos numéricos decimales
                        diccionario[clave] = self.limpiarCampoNumericoDecimal(str(valor))
                    
                    if 'last_update' == clave:
                        # Limpiar campos de fecha
                        diccionario[clave] = self.limpiarCampoFecha(str(valor))

        if self.name == 'inventory':
            for diccionario in self.df_hoja:
                for clave, valor in diccionario.items():
                    if 'inventory_id' == clave or 'film_id' == clave or 'store_id' == clave:
                        # Limpiar campos numéricos
                        diccionario[clave] = self.limpiarCampoNumerico(str(valor))

                    if 'last_update' == clave:
                        # Limpiar campos de fecha
                        diccionario[clave] = self.limpiarCampoFecha(str(valor))
        
        if self.name == 'customer':
            # Validar secuencia de direcciones comenzando desde el valor 5
            self.df_hoja = self.validar_secuencia_consecutiva(self.df_hoja, "address_id", inicio=5)
            for diccionario in self.df_hoja:
                for clave, valor in diccionario.items():
                    if 'customer_id' == clave or 'store_id' == clave or 'active' == clave:
                        # Limpiar campos numéricos
                        diccionario[clave] = self.limpiarCampoNumerico(str(valor))

                    if 'email' == clave:
                        # Limpiar correos electrónicos
                        diccionario[clave] = self.limpiarCorreo(str(valor))

                    if 'first_name' == clave or 'last_name' == clave:
                        # Convertir nombres a mayúsculas
                        diccionario[clave] = self.textoMayusculas(str(valor))

                    if 'create_date' == clave or 'last_update' == clave:
                        # Limpiar campos de fecha
                        diccionario[clave] = self.limpiarCampoFecha(str(valor))

        if self.name == 'rental':
            for diccionario in self.df_hoja:
                for clave, valor in diccionario.items():
                    if 'rental_id' == clave or 'inventory_id' == clave or 'customer_id' == clave or 'staff_id' == clave:
                        # Limpiar campos numéricos
                        diccionario[clave] = self.limpiarCampoNumerico(str(valor))

                    if 'rental_date' == clave or 'return_date' == clave or 'last_update' == clave:
                        # Limpiar campos de fecha
                        diccionario[clave] = self.limpiarCampoFecha(str(valor))

        # Retornar el DataFrame transformado
        return self.df_hoja
