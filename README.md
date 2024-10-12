# Protecto Films
## Clonar repositorio

Para clonar el repositorio ejecutamos los siguiente comandos en Git bash
```
$ git init
$ git clone https://github.com/YOUR-USERNAME/films_project.git
```

## Crear entorno virtual

Para crear nuestro entorno virtual ejecutamos el siguiente comando en nuestra terminal.

***Para nuestro caso es Windows, para el caso de Mac o Linux puedes encontrar como hacerlo [aqui.](https://www.programaenpython.com/miscelanea/crear-entornos-virtuales-en-python/)***

```
python -m venv film-env
```
luego para activar el entorno ejecutamos
```
.\film-env\Scripts\activate
```
## Instalación librerias requeridas

Para instalar todas las librerias basta con ejecutar el siguiente codigo en la terminal, el cual instala nuestro archivo [requirements.txt](https://github.com/Juanma1023/films_project/blob/main/requirements.txt)

```
pip install -r requirements.txt
```
## Ejecución del programa.

Basta ejecutar el archivo main.py para limpiar nuestra base de datos anexada, la cual generara una nueva base de datos limpia
```
