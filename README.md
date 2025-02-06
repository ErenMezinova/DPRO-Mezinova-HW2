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
from pyspark.sql import window

df = spark.read.option('inferSchema','true').option('header', 'true').csv("/opt/bitnami/spark/covid-data.csv")
df.createOrReplaceTempView("df_view")
```

---------------------------------------------------------------------------------------------------------------
**1. Задание: Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)**

Решение: так как точная дата не указана, то пусть целевая дата - 31.03.2021. Так как не указано, как считать процент ПЕРЕБОЛЕВШИХ, то пусть будет по следующей формуле. (total_cases - total_deaths) / total_cases * 100 - число заболевших, но выживших делим на всех заболевших

```
df_march31 = spark.sql('''SELECT *
           FROM df_view
	   WHERE date = to_date('3/31/2021', 'M/dd/yyyy') 
        ''')

df_march31.select('iso_code', 'location', F.round((F.col('total_cases') - F.col('total_deaths')) / F.col('total_cases') *100, 2).alias('survivors_percent')).select('*').orderBy(F.col('survivors_percent').desc()).show(15)

```

![Image](https://github.com/user-attachments/assets/cc18c227-cd34-4f0f-b2ab-7fd551e76db0)


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

![Image](https://github.com/user-attachments/assets/96400f85-a014-4a56-8d5b-d28cb7baee0b)


---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
**3. Задание: Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)**

Решение:

```
df_lw_rus = df_lastweek.select('date', 'location', 'new_cases').where(F.col('location').startswith('Rus'))

df_lwrus_window = Window.partitionBy('location').orderBy(asc('date'))
df_lw_rus = df_lw_rus.withColumn('yesterday_cases', lag('new_cases').over(df_lwrus_window))
df_lw_rus = df_lw_rus.withColumn('delta', F.col('new_cases') - F.col('yesterday_cases'))

df_lw_rus.select('date', 'yesterday_cases', 'new_cases',  'delta').show()
```

![Image](https://github.com/user-attachments/assets/306b9ea8-799f-44a3-91f9-089719c39054)


---------------------------------------------------------------------------------------------------------------
