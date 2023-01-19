# Importamos las librerias necesarias
"""
Pandas: Manejo de Dataframes
Fastapi: Implementación de la API
PlainTextResponde: Output de la información en formato texto
Pandasql: Consultas sobre dataframes mediante lenguaje SQL
"""
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pandasql import sqldf

# Generamos la documentación de nuestra API a efectos de que el usuario conozca
# qué consultas puede realizar y qué parámetros debe ingresar
tags_metadata = [
    {
        "name": "Contar palabras",
        "description": "Cuenta cantidad de titulos conteniendo la keyword \
        solicitada. Requiere el ingreso de una **keyword** a buscar y \
        una **plataforma** (netflix, disney, hulu, amazon). En todos los \
        casos se debe ingresar en minúsculas",
    },
    {
        "name": "Cantidad puntaje mínimo",
        "description": "Cuenta cantidad de **películas** que superan el \
        score indicado para cierto año y plataforma. Requiere el \
        ingreso del **año** en formato AAAA, el **score** como un \
        entero del 0 a 99 y la **plataforma** (netflix, disney, hulu, amazon)",
    },
    {
        "name": "Segunda mayor score",
        "description": "Devuelve la segunda **película** con mayor score \
        para una plataforma. En caso de empate, utiliza el orden alfabético.\
        Requiere ingresar la **plataforma** (netflix, disney, hulu, amazon) \
        en minúsculas",
    },
    {
        "name": "Mayor duración",
        "description": "Informa cual es la **película** con mayor duración \
        en minutos o la **serie** con mas temporadas, de acuerdo al tipo de \
        duración ingresado. Requiere ingresar el **año** en formato AAAA, \
        el **tipo de duración** (min / season) y la **plataforma** (netflix, \
        disney, hulu, amazon), todo en minúsculas",
    },
    {
        "name": "Cantidad por rating",
        "description": "Devuelve la cantidad total de **películas** y **series** \
        de acuerdo al **rating** ingresado (16, 13+, 16+, 18+, 7+, ages_16_, \
        ages_18_, all, all_ages, g, nc-17, not rated, not_rate, nr, pg, pg-13, r, \
        tv-14, tv-g, tv-ma, tv-nr, tv-pg, tv-y, tv-y7, tv-y7-fv, unrated, ur)",
    },
]

# Inicializamos la API
app = FastAPI(openapi_tags=tags_metadata)


# Importamos el dataset unificado
moviesdb = pd.read_csv(
    "https://raw.githubusercontent.com/adelgerbo/Datasets/main/movies_database.csv"
)


# Comenzamos con las consultas al dataframe. Esta función nos evitará pasar variables
# globales cada vez que utilicemos un objeto

pysqldf = lambda q: sqldf(q, globals())


# Definimos el mensaje a mostrar cuando el usuario ingrese al root de nuestra API
@app.get("/")
def docs():
    """
    Muestra un mensaje al usuario cuando ingresa al root
    """
    return "Por favor ingrese a https://c0q8v6.deta.dev/docs para acceder a la documentación de la aplicación"


# Definimos la función para el siguiente requerimiento: Cantidad de veces que aparece
# una keyword en el título de peliculas/series, por plataforma
@app.get(
    "/get_word_count/{plataforma}/{keyword}",
    response_class=PlainTextResponse,
    tags=["Contar palabras"],
)
def get_word_count(keyword: str, plataforma: str):
    """
    Cuenta cantidad de titulos conteniendo la keyword solicitada.
    Requiere el ingreso de una **keyword** a buscar y una **plataforma**
    (netflix, disney, hulu, amazon).
    En todos los casos se debe ingresar en minúsculas
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT COUNT(title)
        FROM moviesdb
        WHERE title LIKE '%"""
        + keyword
        + """%'
        AND id LIKE '"""
        + plat
        + """' """
    )
    veces = pysqldf(query)
    return veces.to_string(index=False, header=False)


# Definimos la función para el siguiente requerimiento: Cantidad de películas
# por plataforma con un puntaje mayor a XX en determinado año
@app.get(
    "/get_score_count/{plataforma}/{score}/{anio}",
    response_class=PlainTextResponse,
    tags=["Cantidad puntaje mínimo"],
)
def get_score_count(plataforma: str, score: str, anio: str):
    """
    Cuenta cantidad de **películas** que superan el score indicado para cierto año y plataforma.
    Requiere el ingreso del **año** en formato AAAA, el **score** como un entero del 0 a 99
    y la **plataforma** (netflix, disney, hulu, amazon)
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT COUNT(title)
        FROM moviesdb
        WHERE score > """
        + score
        + """
        AND release_year = """
        + anio
        + """
        AND id LIKE '"""
        + plat
        + """'
        AND type = "movie" """
    )
    cantidad = pysqldf(query)
    return cantidad.to_string(index=False, header=False)


# Definimos la función para el siguiente requerimiento: La segunda película
# con mayor score para una plataforma determinada,
# según el orden alfabético de los títulos.
@app.get(
    "/get_second_score/{plataforma}",
    response_class=PlainTextResponse,
    tags=["Segunda mayor score"],
)
def get_second_score(plataforma: str):
    """
    Devuelve la segunda **película** con mayor score para una plataforma.
    En caso de empate, utiliza el orden alfabético.
    Requiere ingresar la **plataforma** (netflix, disney, hulu, amazon) en minúsculas.
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT title
        FROM moviesdb
        WHERE id LIKE '"""
        + plat
        + """'
        AND type = "movie"
        ORDER BY score DESC, title ASC
        LIMIT 1,1"""
    )
    segunda = pysqldf(query)
    return segunda.to_string(index=False, header=False)


# Definimos la función para el siguiente requerimiento: Película que más duró
# en minutos o Serie que más duró en temporadas, según año, plataforma y tipo de duración
# El tipo de duración indica si se consulta por películas o series
@app.get(
    "/get_longest/{plataforma}/{tipo_duracion}/{anio}",
    response_class=PlainTextResponse,
    tags=["Mayor duración"],
)
def get_longest(plataforma: str, tipo_duracion: str, anio: str):
    """
    Informa cual es la **película** con mayor duración en minutos o la **serie**
    con mas temporadas, de acuerdo al tipo de duración ingresado.
    Requiere ingresar el **año** en formato AAAA, el **tipo de duración** (min / season)
    y la **plataforma** (netflix, disney, hulu, amazon), todo en minúsculas
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT title
        FROM moviesdb
        WHERE id LIKE '"""
        + plat
        + """'
        AND release_year = '"""
        + anio
        + """'
        AND duration_type = '"""
        + tipo_duracion
        + """'
        AND duration_int = (SELECT MAX(duration_int)
        FROM moviesdb
        WHERE id LIKE '"""
        + plat
        + """'
        AND release_year = '"""
        + anio
        + """'
        AND duration_type = '"""
        + tipo_duracion
        + """')
        """
    )

    maslarga = pysqldf(query)
    return maslarga.to_string(index=False, header=False)


# Definimos la función para el siguiente requerimiento: Cantidad de
# series y películas por rating
@app.get(
    "/get_rating_count/{rating}",
    response_class=PlainTextResponse,
    tags=["Cantidad por rating"],
)
def get_rating_count(rating: str):
    """
    Devuelve la cantidad total de **películas** y **series** de acuerdo al **rating**
    ingresado (16, 13+, 16+, 18+, 7+, ages_16_, ages_18_, all, all_ages, g, nc-17,
    not rated, not_rate, nr, pg, pg-13, r, tv-14, tv-g, tv-ma, tv-nr, tv-pg, tv-y,
    tv-y7, tv-y7-fv, unrated, ur)
    """
    query = (
        """SELECT count(title)
        FROM moviesdb
        WHERE rating = '"""
        + rating
        + """' """
    )
    total = pysqldf(query)
    return total.to_string(index=False, header=False)
