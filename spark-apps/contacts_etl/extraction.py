import sys
from pgdi.util.hash import HashControl
from pgdi.util.etl import ETLUtils
from pgdi.util.files import extractTextFromBinaryField
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf, lit, UserDefinedFunction
from pyspark.sql.types import DateType, StringType

hdfs_url = sys.argv[1]

# Recupera la sesión de Spark
spark = SparkSession \
        .builder \
        .config("spark.sql.debug.maxToStringFields",10000000) \
        .getOrCreate()

def extract():

    # Se procede a realizar una conexión JDBC a MySQL ubicado en GCP
    # y obtiene el dataframe con base a una consulta
    df_jdbc = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://XX.XX.XX.XX:3306/contactsdb") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("user", "root") \
            .option("password", "pass") \
            .option("dbtable", "(SELECT * FROM contacts) as c") \
            .option("fetchSize", 5000) \
            .load()
    
    # Guarda los datos tal cual como vienen de la fuente
    df_jdbc.write.mode("overwrite").parquet(f"{hdfs_url}/datalake/Contacts_raw")

def main():
    extract()


if __name__ == '__main__':
    main()