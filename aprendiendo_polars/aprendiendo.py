import polars as pl

gender = pl.read_csv("aprendiendo_polars/gender_submission.csv")
test = pl.read_csv("aprendiendo_polars/test.csv")
train = pl.read_csv("aprendiendo_polars/train.csv")

#Imprimir las primeras 5 filas de cada DataFrame
print(gender.columns)
print(test.columns)
print(train.columns)

#quitar los nullos de una columna y llenarlos con No
train = train.with_columns(pl.col("Cabin").fill_null("No"))
#print(train.head())

test = test.with_columns(pl.col("Cabin").fill_null("No"))
#print(test.head())

#filtrando los mayores de edad

eje_mayores = train.filter(pl.col("Age")>=18)
#print(eje_mayores.head())

#filtrando mayores de edad y de tercera clase

eje_mayores_tercera_clase = train.filter((pl.col("Age")>=18) & (pl.col("Pclass") == 3)) #podemos colocar & o and y si queremos utilizar el o debemos colocar | o or
#print(f"ejemplo tercera clase: {eje_mayores_tercera_clase.head()}")

#combinar dataframes 

test = test.join(gender, on="PassengerId", how="left")
#print(test.head())

#unir dataframes

combinado = pl.concat([train, test], how = "align") #podemos colocar horizontal o vertical, align, diagonal dependiendo de lo que se necesite tiene varias opciones
#print(combinado.head())

#agrupar por una columna y contar los valores
conteo_clase = combinado.group_by("Pclass").agg(
    pl.col("PassengerId").count()
)
#print(conteo_clase)

#agrupar por una columna y dar varios valores
conteo_clase = combinado.group_by("Pclass").agg(
    pl.col("PassengerId").count().alias("Conteo"), #podemos colocarle un alias para nombrar la columna
    pl.col("Age").mean().alias("Promedio edad"),
    pl.col("Fare").sum().alias("Suma tarifa")
)
#print(conteo_clase)

#crear una nueva columna y calcular la edad promedio por clase
datos_con_porcentaje = combinado.with_columns(
    ((pl.col("Fare").over("Pclass")/ pl.col("Fare").sum().over("Pclass"))*100).alias("Ingreso promedio por clase")#el over es para agrupar por una columna
)

print(datos_con_porcentaje)
print(conteo_clase)

#convertir columnas a fecha
#combinado.with_columns(pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d"))

#extraer partes de las fechas
#combinado.with_columns(pl.col("Date").dt.year()) o combinado.with_columns(pl.col("Date").dt.month()) o combinado.with_columns(pl.col("Date").dt.day())

#filtrar por mes 
#combinado.filter(pl.col("Date").dt.month() == 1)

#limpiar y procesar texto 
#reemplazar texto: pl.col("columna").str.replace("viejo", "nuevo")
#Dividir cadenas: pl.col("columna").str.split("_")
#Convertir a mayúsculas o minúsculas: pl.col("columna").str.to_uppercase()
#Comprobar si una cadena contiene un patrón: pl.col("columna").str.contains("patron")


# Polars solo lee la "receta" de cómo cargar el archivo
#lazy_df = pl.scan_csv("ventas_gigantes.csv") el scan_csv es para cargar archivos grandes

# Polars añade estos pasos a la "receta", sin ejecutar nada
#lazy_df = lazy_df.filter(pl.col("producto") == "laptop")
#lazy_df = lazy_df.group_by("producto").agg(pl.sum("total_venta"))

# Solo aquí, Polars ejecuta todos los pasos de la manera más rápida
#reporte = lazy_df.collect()

# Script para convertir un archivo de Excel a Parquet
# import pandas as pd

# Leer el archivo de Excel
# df_pandas = pd.read_excel("mi_archivo_gigante.xlsx")

# Guardar en formato Parquet, que Polars puede leer de forma nativa y rápida
#df_pandas.to_parquet("mi_archivo_gigante.parquet")

# Ahora, en tu código principal, puedes usar Polars para leerlo de forma perezosa
#lazy_df = pl.scan_parquet("mi_archivo_gigante.parquet")


#tabla dinamica de clase por sexo

tabla_dinamica = combinado.pivot(index="Pclass", columns="Sex", values="Fare", aggregate_function="sum")
print(tabla_dinamica)

#mirar repetidos
repetidos = combinado.is_duplicated()
print(repetidos)

#dejar valores unicos
unicos = combinado.unique()

