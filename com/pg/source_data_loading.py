# read data from mysql - transactionsync, create a df out of it
# add a column 'ins-dt' - current_date()
# write the df in s3 partitioned by ins_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils.utils as ut
import yaml
import os.path
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType

if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('f3.s3a.access.key', app_secret['s3_conf']['access_key'])
    hadoop_conf.set('f3.s3a.secret.key', app_secret['s3_conf']['secret_access_key'])

    src_list = app_conf['source_list']

    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            # Read from mysql db
            txn_df = ut.read_from_mysql(src_conf, app_secret, spark) \
                .withColumn('ins_dt', current_date())

            txn_df.show()

            txn_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_dir'] + '/' + src)

        elif src == 'OL':
            # read data from sftp - receipts, create a df out of it
            # add a column 'ins-dt' - current_date()
            # write the df in s3 partitioned by ins_dt

            receipts_df = ut.read_from_sftp(spark, src_conf, app_secret,
                                            os.path.abspath(
                                                current_dir + "/../../" + app_secret['sftp_conf']['pem']),
                                            ) \
                .withColumn('ins_dt', current_date())

            receipts_df.show()

            receipts_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_dir'] + '/' + src)

        elif src == 'CP':
            # read data from s3 - kcextract, create a df out of it
            # add a column 'ins-dt' - current_date()
            # write the df in s3 partitioned by ins_dt

            kc_df = ut.read_from_s3(spark, src_conf) \
                .withColumn('ins_dt', current_date())

            kc_df.show(5, False)

            kc_df.write \
                .mode('append') \
                .partitionBy('ins_dt')\
                .parquet("s3a://" + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_dir'] + '/' + src)

        elif src == 'ADDR':
            # read data from mongodb - transactionsync, create a df out of it
            # add a column 'ins-dt' - current_date()
            # write the df in s3 partitioned by ins_dt

            cus_df = ut.read_from_mongo(spark, src_conf) \
                .withColumn('ins_dt', current_date())

            cus_df.show(5, False)

            cus_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_dir'] + '/' + src)

