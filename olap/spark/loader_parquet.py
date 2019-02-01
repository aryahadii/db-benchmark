from loader import load_data


def write_parquet(df, file_name):
    df.write.mode('overwrite').parquet(
        'hdfs://namenode:8020/{}.parquet'.format(file_name),
    )


load_data(write_func=write_parquet)
