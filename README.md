<h1 align=center> HENRY’S LABS </h1>

<h2 align=center>PROYECTO INDIVIDUAL I -- DATA ENGINEERING<br>
    Alejandro del Gerbo Actis</h2>


### **Temática**

El proyecto consistía en situarse en el rol de Data Engineer, a quien como miembro del equipo de una empresa, el Tech Lead le solicita realizar un proceso de ETL sobre cuatro datasets proporcionados, conteniendo información relativa a los catálogos de series y películas de cuatro plataformas de streaming (Netflix, Hulu, Amazon Prime Video y Disney).

Como segunda parte del requerimiento, se solicitaba elaborar una API a efectos de disponibilizar los datos de manera online, los cuales debían ser accedidos mediante cinco consultas predefinidas.

Por último, se solicita documentar todo el proceso y el funcionamiento de la API, y efectuar un video que sería remitido al Tech Lead que nos encargó el proyecto para que nos efectúe un feedback sobre el mismo.

### **Detalles del requerimiento**

-   **Transformaciones:** Se solicitó efectuar las siguientes transformaciones sobres los datos provistos:
    -   Generar campo id: Cada id se debía componer de la primera letra del nombre de la plataforma, seguido del show id ya presente en los datasets (ejemplo: para títulos de Amazon = as123)
    -   Los valores nulos del campo rating debía reemplazarse por el string “G” (corresponde al maturity rating: “general for all audiences”
    -   De haber fechas, debían tener el formato AAAA-mm-dd
    -   Los campos de texto debían estar en minúsculas, sin excepciones
    -   El campo duration debía convertirse en dos campos: duration_int y duration_type. El primero con formato integer y el segundo string, indicando la unidad de medición de duración; min (minutos) o season (temporadas)
-   **Consultas:**
    -   Cantidad de tìtulos (películas o series) en los que aparece al menos una vez una keyword provista, por plataforma
    -   Cantidad de películas por plataforma con un puntaje mayor al valor provisto en determinado año
    -   La segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos.
    -   Película de mayor duración o serie con más temporadas, según año, plataforma y tipo de duración (minutos para películas o seasons para series)
    -   Cantidad de series y películas por rating

### **Herramientas utilizadas**

-   **[Python](https://www.python.org/)**: lenguaje de programación utilizado, que brinda acceso a librerías apropiadas para realizar la tarea encomendada de manera eficaz y eficiente.
-   **[Pandas](https://pandas.pydata.org/):** librería que nos permite el acceso a los archivos csv provistos, su conversión en dataframes, la transformación de los datos y la posterior exportación de los mismos en un único archivo, el cual luego será utilizado por la API para disponibilizar los datos.
-   **[Pandasql](https://pypi.org/project/pandasql/):** librería que nos permite efectuar consultas con lenguaje SQL sobre Dataframes de Pandas.
-   **[FastApi](https://fastapi.tiangolo.com/):** framework para la construcción de la API en Python.
-   **[Uvicorn](https://www.uvicorn.org/):** nos permite efectuar pruebas para controlar el funciomaniento de la API de manera local previa a su despliegue en Deta.
-   **[Deta](https://www.deta.sh/):** plataforma online y gratuita, que nos permite disponibilizar tanto la API como los datos para que puedan ser consultados por el usuario mediante su software.

### **Tareas realizadas**

-   Se colocaron los datasets provistos en el directorio Datasets y se realizó una vista previa de su contenido
-   Se importó la librería Pandas para ingestar los cuatro archivos csv (uno por cada plataforma de streaming), se efecutaron las transformaciones sobre los datos solicitadas y se exportó a un único archivo csv conteniendo los catálogos de las cuatro plataformas de streaming. Este punto puede revisarse en detalle accediendo al notebook que contiene el código comentado y las sucesivas visualizaciones y resultados ( <https://github.com/adelgerbo/Proyecto-individual-1-Henry/blob/main/ETL.ipynb> ).
-   Se subió el dataset unificado en csv al repositorio de Github, para que pueda ser accedido por la API para ingestar los datos que luego serán objeto de consulta.
-   Se desarrolló la API utilzando FastApi. Se incluyeron en la misma las consultas en lenguaje SQL sobre el dataframe de Pandas, utilizando la librería Pandasql. Puede consultarse el código comentando en el archivo main.py ( <https://github.com/adelgerbo/Proyecto-individual-1-Henry/blob/main/Fastapideta/main.py> )
-   Se hicieron las pruebas de manera local previo a ser desplegada de manera online, utilizando Uvicorn, y comprobando que las consultas arrojaban los resultados esperados.
-   Se hizo el deploy de la API en Deta y se testearon las diferentes consultas. Se utilizó tanto el visor online provisto por Deta como los logs para ir evaluando el funcionamiento.
-   Se confeccionó la documentación relativa al uso de la API dentro de su misma interfaz online, de modo que pueda ser consultada ingresando a <https://c0q8v6.deta.dev/docs> . En caso de ingresar al root del dominio, un mensaje informará que en dicha URL se puede acceder a la documentación online.
-   Se grabó, editó y publicó el video solicitado en el cual se expone la API en funcionamiento, efectuando en vivo todas las consultas solicitadas. Al mismo se accede mediante la siguiente URL: https://www.youtube.com/watch?v=hgNbuqcuIKo
-   Se preparó el Readme.md con la documentación.

### **Instrucciones para el uso de la API**

1.  A efectos de consultar la documentación, se debe ingresar a la siguiente URL: <https://c0q8v6.deta.dev/docs> . Allí pueden realizarse tambien consultas manualmente utilizando la interfaz disponible.
2.  Para efecutar las consultas solicitadas, se coloca luego de la URL, el nombre de la consulta y los parámetros solicitados, de acuerdo al siguiente detalle:
-   /get_word_count/{plataforma}/{keyword}: Cuenta cantidad de titulos conteniendo la keyword solicitada. Requiere el ingreso de una **keyword** a buscar y una **plataforma** (netflix, disney, hulu, amazon). A efectos de este proyecto, se nos indicó que si la keyword informada se encuentra dentro de una palabra mas grande, se incluye como resultado positivo (ejemplo: “Cloverfield” se considera como “love” también).  
    En todos los casos se debe ingresar en minúsculas.   
    Ejemplo: <https://c0q8v6.deta.dev/get_word_count/netflix/love> (Cantidad de películas y series de Netflix que contienen la palabra “love”)
-   /get_score_count/{plataforma}/{score}/{anio}: Cuenta cantidad de **películas** que superan el score indicado para cierto año y plataforma. Requiere el ingreso del **año** en formato AAAA, el **score** como un entero del 0 a 99 y la **plataforma** (netflix, disney, hulu, amazon).**  
    Ejemplo:** <https://c0q8v6.deta.dev/get_score_count/netflix/85/2010> **(Cantidad de películas de Netflix lanzadas en el año 2010 cuyo puntaje es superior a 85)**
-   /get_second_score/{plataforma}: Devuelve la segunda **película** con mayor score para una plataforma. En caso de empate, utiliza el orden alfabético. Requiere ingresar la **plataforma** (netflix, disney, hulu, amazon) en minúsculas.  
    Ejemplo: <https://c0q8v6.deta.dev/get_second_score/amazon> (Segunda película de la plataforma Amazon Prime Video con el puntaje mas alto).
-   /get_longest/{plataforma}/{tipo_duracion}/{anio}: Informa cual es la **película** con mayor duración en minutos o la **serie** con mas temporadas, de acuerdo al tipo de duración ingresado. Requiere ingresar el **año** en formato AAAA, el **tipo de duración** (min / season) y la **plataforma** (netflix, disney, hulu, amazon), todo en minúsculas.  
    Ejemplo: <https://c0q8v6.deta.dev/get_longest/netflix/min/2016> (Película de mayor duración en minutos en la plataforma Netflix lanzada en el año 2016. De haber utilizado el parámetro “season” en lugar de “min”, el resultado hubiera hecho referencia a la o las series con mayor cantidad de temporadas).
-   /get_rating_count/{rating}: Devuelve la cantidad total de **películas** y **series** de acuerdo al **rating** ingresado (16, 13+, 16+, 18+, 7+, ages_16_, ages_18_, all, all_ages, g, nc-17, not rated, not_rate, nr, pg, pg-13, r, tv-14, tv-g, tv-ma, tv-nr, tv-pg, tv-y, tv-y7, tv-y7-fv, unrated, ur).  
    Ejemplo: <https://c0q8v6.deta.dev/get_rating_count/18>+ (Cantidad de películas y series cuyo rating es “18+”).



### **Datos sobre el autor**

**Alejandro del Gerbo Actis**

adelgerbo@gmail.com
