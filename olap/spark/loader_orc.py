from .loader import load_data


def write_orc(df, file_name):
    df.write.mode('overwrite').orc('hdfs://namenode:8020/{}.orc'.format(file_name))


load_data(write_func=write_orc)
