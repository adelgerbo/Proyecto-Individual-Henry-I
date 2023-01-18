# Importamos las librerias necesarias
import pandas as pd
from pandasql import sqldf
from fastapi import FastAPI


app = FastAPI()


# Importamos los archivos CSV, uno para cada servicio de streaming
Amazon = pd.read_csv(
    "https://raw.githubusercontent.com/adelgerbo/Datasets/main/amazon_prime_titles-score.csv"
)
Disney = pd.read_csv(
    "https://raw.githubusercontent.com/adelgerbo/Datasets/main/disney_plus_titles-score.csv"
)
Hulu = pd.read_csv(
    "https://raw.githubusercontent.com/adelgerbo/Datasets/main/hulu_titles-score%20(2).csv"
)
Netflix = pd.read_csv(
    "https://raw.githubusercontent.com/adelgerbo/Datasets/main/netflix_titles-score.csv"
)


# Verificamos la importaciòn correcta de los datasets
Amazon.head()


Disney.head()


Hulu.head()


Netflix.tail()


"""
Generamos el campo id: Cada id se compondrá de la primera letra del
nombre de la plataforma, seguido del show_id ya presente en los 
datasets (ejemplo para títulos de Amazon = as123)
"""
Amazon.insert(loc=0, column="id", value="a" + Amazon["show_id"])

Disney.insert(loc=0, column="id", value="d" + Disney["show_id"])

Hulu.insert(loc=0, column="id", value="h" + Hulu["show_id"])

Netflix.insert(loc=0, column="id", value="n" + Netflix["show_id"])


# Comprobamos la correcta inserciòn del nuevo campo
Netflix.head()


"""
Juntamos todos los datasets en uno solo, ya que contamos 
con el campo id para diferenciar el servicio, y las
transformaciones a continuación se aplican a todos por igual
"""

moviesdb = Amazon.append(Disney).append(Hulu).append(Netflix)


# Verificamos el resultado
moviesdb.head()


moviesdb.tail()


# Verificamos existencia y cantidad de nulos por columna en el dataframe
moviesdb.isnull().sum()


"""
Reemplazamos los valores nulos del campo "Rating" por la letra "G"
(corresponde al maturity rating "General por all audiences")
"""
moviesdb["rating"].fillna("G", inplace=True)


# Controlamos la correcciòn sobre la columna "rating"
moviesdb.isnull().sum()


# Visualizamos el dataframe modificado
moviesdb.head(30)


# Cambiamos el formato de la columna "date_added" a AAAA-mm-dd
moviesdb["date_added"] = pd.to_datetime(moviesdb["date_added"])


# Verificamos
moviesdb.tail()


# Pasamos a minúsculas los campos de texto
moviesdb["type"] = moviesdb["type"].str.lower()
moviesdb["title"] = moviesdb["title"].str.lower()
moviesdb["director"] = moviesdb["director"].str.lower()
moviesdb["cast"] = moviesdb["cast"].str.lower()
moviesdb["country"] = moviesdb["country"].str.lower()
moviesdb["rating"] = moviesdb["rating"].str.lower()
moviesdb["duration"] = moviesdb["duration"].str.lower()
moviesdb["listed_in"] = moviesdb["listed_in"].str.lower()
moviesdb["description"] = moviesdb["description"].str.lower()


# Verificamos
moviesdb.head()


# Separamos el campo "duration" en 2 columnas
splitcolumns = moviesdb["duration"].str.split(" ", n=1, expand=True)

# Insertamos una columna con las cantidades
moviesdb["duration_int"] = splitcolumns[0]

# Insertamos una columna con la unidad de medición
moviesdb["duration_type"] = splitcolumns[1]


# Reemplazamos "seasons" por "season" en la columna "duration_type"
moviesdb["duration_type"] = moviesdb["duration_type"].str.replace("seasons", "season")


# Cambiamor el formato del campo "duration_int" a integer
moviesdb["duration_int"] = moviesdb["duration_int"].astype("Int64")


# Verificamos
moviesdb.head(10)


moviesdb.info()


"""
Comenzamos con las consultas al dataframe. Esta función nos evitará pasar variables
globales cada vez que usemos un objeto
"""
pysqldf = lambda q: sqldf(q, globals())


"""
Definimos la función para el siguiente requerimiento: Cantidad de veces que aparece 
una keyword en el título de peliculas/series, por plataforma
"""


@app.get("/keyword/{keyword}/{plataforma}")
def get_word_count(keyword, plataforma):
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"

    query = (
        """SELECT SUM (LENGTH(title) - LENGTH(REPLACE(title,'"""
        + keyword
        + """',""))) / LENGTH('"""
        + keyword
        + """')
        as cantidad
        FROM moviesdb
        WHERE id LIKE '"""
        + plat
        + """' """
    )
    veces = pysqldf(query)
    return veces.to_string(index=False, header=False)
