from pyspark.sql import Row
from pyspark.sql import functions as F

from dependencies.spark_conn import start_spark

def main():


    spark, log, config = start_spark(
        app_name='my_etl_job',spark_config={'spark.sql.shuffle.partitions':2})
    print("Hello WOrld")
    log.warn('etl_job is up-and-running')
    data = extract_data(spark)
    data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data_transformed)

    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark):
    df = (
        spark
        .read
        .parquet('employees'))

    return df

def transform_data(df, steps_per_floor_):
    df_transformed = (
        df
        .select(
            F.col('id'),
            F.concat_ws(
                ' ',
                F.col('first_name'),
                F.col('second_name')).alias('name'),
               (F.col('floor') * F.lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed

def load_data(df):
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite'))
    return None

if __name__ == '__main__':
    main()
