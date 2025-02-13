# DPRO-Mezinova-HW2
Домашнее задание по теме «Spark SQL»

_Подготовка:_

```
docker cp covid-data.csv 20e2a09871909ec51f1435eff77b5eaf7b603b59714e16dadda416fb495f89f5:/opt/bitnami/spark
docker exec -it 20e2a09871909ec51f1435eff77b5eaf7b603b59714e16dadda416fb495f89f5 bash

export PYTHONPATH=/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/bitnami/spark/python/:/opt/bitnami/spark/python/:
export PYTHONSTARTUP=/opt/bitnami/spark/python/pyspark/shell.py 
exec "${SPARK_HOME}"/bin/spark-submit pyspark-shell-main
```


```
from pyspark.sql import functions as F
from pyspark.sql import Window

df = spark.read.option('inferSchema','true').option('header', 'true').csv("/opt/bitnami/spark/covid-data.csv")
df.createOrReplaceTempView("df_view")
```

---------------------------------------------------------------------------------------------------------------
**1. Задание: Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)**

Решение: так как точная дата не указана, то пусть целевая дата - 31.03.2021. Так как не указано, как считать процент ПЕРЕБОЛЕВШИХ, то пусть будет по следующей формуле. (total_cases - total_deaths) / **population** * 100 - число заболевших, но выживших делим на население страны

```
df_march31 = spark.sql('''SELECT *
           FROM df_view
	   WHERE date = to_date('3/31/2021', 'M/dd/yyyy') 
        ''')

df_march31.select('iso_code', 'location', F.round((F.col('total_cases') - F.col('total_deaths')) / F.col('population') *100, 2).alias('survivors_percent')).select('*').orderBy(F.col('survivors_percent').desc()).show(15)

```

```
+--------+-------------+-----------------+
|iso_code|     location|survivors_percent|
+--------+-------------+-----------------+
|     AND|      Andorra|             15.4|
|     MNE|   Montenegro|            14.32|
|     CZE|      Czechia|            14.06|
|     SMR|   San Marino|            13.69|
|     SVN|     Slovenia|            10.18|
|     LUX|   Luxembourg|             9.73|
|     ISR|       Israel|             9.55|
|     USA|United States|             9.04|
|     SRB|       Serbia|             8.75|
|     BHR|      Bahrain|             8.46|
|     PAN|       Panama|             8.09|
|     EST|      Estonia|             7.95|
|     PRT|     Portugal|             7.89|
|     SWE|       Sweden|             7.84|
|     LTU|    Lithuania|             7.81|
+--------+-------------+-----------------+
```

---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
**2. Задание: Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)**

Решение: выбираем данные за последнюю неделю марта 2021. По каждой стране и дате находим максимальное кол-во новых случаев, в случае, если в данных окажется несколько записей за одну дату и страну. Нумеруем строки по каждой стране в порядке убывания new_cases. Итоговый датасет формируем из первых строк по каждой стране. 

```
df_lastweek = spark.sql('''SELECT *
           FROM df_view
	   WHERE date BETWEEN (to_date('3/25/2021', 'M/dd/yyyy'))  AND (to_date('3/31/2021', 'M/dd/yyyy')) AND
		 NOT continent IS NULL	
        ''')
```

```
df_lw_max = df_lastweek.groupBy('date', 'location').max('new_cases')

df_lwm_window = Window.partitionBy('location').orderBy(desc('max(new_cases)'))
df_lw_max = df_lw_max.withColumn('row_number', row_number().over(df_lwm_window))

df_lw_max.select('date', 'location', 'max(new_cases)').filter(F.col('row_number')==1).orderBy(F.col('max(new_cases)').desc()).show(10)
```

```
+----------+-------------+--------------+
|      date|     location|max(new_cases)|
+----------+-------------+--------------+
|2021-03-25|       Brazil|      100158.0|
|2021-03-26|United States|       77321.0|
|2021-03-31|        India|       72330.0|
|2021-03-31|       France|       59054.0|
|2021-03-31|       Turkey|       39302.0|
|2021-03-26|       Poland|       35145.0|
|2021-03-31|      Germany|       25014.0|
|2021-03-26|        Italy|       24076.0|
|2021-03-25|         Peru|       19206.0|
|2021-03-26|      Ukraine|       18226.0|
+----------+-------------+--------------+
```

---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
**3. Задание: Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)**

Решение: берем подготовленные данные из второго задания - данные за последнюю неделю марта 2021 года и оставляем только Россию. 

```
df_lw_rus = df_lastweek.select('date', 'location', 'new_cases').where(F.col('location').startswith('Rus'))

df_lwrus_window = Window.partitionBy('location').orderBy(F.col('date').asc())

Значения NULL - отсуствующее предыдущее значение для текущей строки преобразуем в 0.0
df_lw_rus = df_lw_rus.withColumn('yesterday_cases', F.coalesce(F.lag('new_cases').over(df_lwrus_window), F.lit(0.0)))

или отсуствующее предыдущее значение для текущей строки заменяем на значение самой текущей строки
df_lw_rus = df_lw_rus.withColumn('yesterday_cases', F.coalesce(F.lag('new_cases').over(df_lwrus_window), F.col('new_cases')))

df_lw_rus = df_lw_rus.withColumn('delta', F.col('new_cases') - F.col('yesterday_cases'))
df_lw_rus.select('date', 'yesterday_cases', 'new_cases',  'delta').show()
```

NULL -> 0.0
```
+----------+---------------+---------+------+
|      date|yesterday_cases|new_cases| delta|
+----------+---------------+---------+------+
|2021-03-25|            0.0|   9128.0|9128.0|
|2021-03-26|         9128.0|   9073.0| -55.0|
|2021-03-27|         9073.0|   8783.0|-290.0|
|2021-03-28|         8783.0|   8979.0| 196.0|
|2021-03-29|         8979.0|   8589.0|-390.0|
|2021-03-30|         8589.0|   8162.0|-427.0|
|2021-03-31|         8162.0|   8156.0|  -6.0|
+----------+---------------+---------+------+
```

NULL -> new_cases
```
+----------+---------------+---------+------+
|      date|yesterday_cases|new_cases| delta|
+----------+---------------+---------+------+
|2021-03-25|         9128.0|   9128.0|   0.0|
|2021-03-26|         9128.0|   9073.0| -55.0|
|2021-03-27|         9073.0|   8783.0|-290.0|
|2021-03-28|         8783.0|   8979.0| 196.0|
|2021-03-29|         8979.0|   8589.0|-390.0|
|2021-03-30|         8589.0|   8162.0|-427.0|
|2021-03-31|         8162.0|   8156.0|  -6.0|
+----------+---------------+---------+------+
```

---------------------------------------------------------------------------------------------------------------
