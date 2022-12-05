# Ejemplo de ETL sobre Spark en infraestructura On-premise

El proyecto se centra en un proceso ETL el cual se realiza en un ambiente de Spark/Hadoop con Pyspark.

Está pensado para ejecutarse en Apache Airflow, se compone del siguiente DAG:

	* contacts_dag.py

y de dos archivos más .py, que son las dos tareas que lo conforman:

	* extraction.py
	* tranformation_load.py

Cabe decir que estos dos últimos archivos son aplicaciones Spark.

## Resumen del funcionamiento

La tarea de extracción obtiene los datos de un MySQL con dirección IP Pública hospedado en la nube. Obtiene los datos, los almacena en un dataframe de PySpark e inmediatamente después se almacenan en un data lake HDFS con formato parquet, al cual se le establece el nombre como **Contacts_raw**

La tarea de tranformation_load, realiza algunas sencillas tranformaciones de tipo de dato y formato de telefonos, para después almacenar los datos **limpios** en un datawarehouse de [Delta Lake](https://delta.io/)

## Configuración

1. Se debe definir dos variables en Apache Airflow:

* PGDI_HADOOP_URL. Indica la URL del HDFS a escribir/leer.
* PYSPARK_APP_HOME. Indica la ubicación física de la carpeta de las aplicaciones de Spark (carpeta spark-apps)

2. Se debe contar con el driver JDBC de Mysql (mysql-connector-java-8.0.21.jar). En este caso es la versión 8 y se encuentra ubicado en la carpeta jars de SPARK_HOME

3. Se debe contar con el jar **delta-core_2.11-0.6.1.jar** y cargarlo como paquete en la tarea *transformation_load.py* en la sesión de Spark (io.delta:delta-core_2.12:1.1.0)
