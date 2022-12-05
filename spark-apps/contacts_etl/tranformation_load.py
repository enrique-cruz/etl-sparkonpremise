import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

hdfs_url = sys.argv[1]

# Recupera la sesión de Spark
spark = SparkSession \
        .builder \
        .config("spark.sql.debug.maxToStringFields",10000000) \
        .getOrCreate()

# Funcion UDF para aplicar una tranformación a los telefonos
@udf(returnType=StringType())
def clean_phone(phone):
    return phone.replace("-","")

def transform():

    # Obtiene los datos del data lake sin procesar
    df_contacts_raw = spark.read.parquet(f"{hdfs_url}/datalake/Contacts_raw")
    
    # Realiza algunas transformaciones a los datos
    df_contacts_transf = df_contacts_raw.withColumn("phone1_int", clean_phone(df_contacts_raw.phone1).cast(IntegerType())) \
                                        .withColumn("phone2_int", clean_phone(df_contacts_raw.phone2).cast(IntegerType())) \
                                        .drop("phone1") \
                                        .drop("phone2") 
    
    df_contacts_transf = df_contacts_transf.withColumn("phone1", df_contacts_transf.phone1_int.cast(IntegerType())) \
                                        .drop("phone1_int")
    df_contacts_transf = df_contacts_transf.withColumn("phone2", df_contacts_transf.phone2_int.cast(IntegerType())) \
                                        .drop("phone2_int")

    # Guarda los datos en formato delta
    df_contacts_transf.write.format("delta") \
        .save(f"{hdfs_url}/warehouse/Contacts")

def main():
    transform()


if __name__ == '__main__':
    main()