from .loader import load_data


def write_avro(df, file_name):
    df.write.format("com.databricks.spark.avro").mode('overwrite').save(
        'hdfs://namenode:8020/{}.avro'.format(file_name)
    )


load_data(write_func=write_avro)
