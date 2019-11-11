from pyspark.sql import functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, json
from pyspark.sql import Row
import re
import sys
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
import os

# from pyspark import SparkConf, SparkContext
# conf = SparkConf()
# conf.setAppName("using_s3")
# conf.setMaster('local')
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5')
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)

# sc =
# spark = SparkSession(sc)

# spark = SparkSession.builder.appName("using_s3").getOrCreate()


# from pyspark.sql import SparkSession
# # initialise sparkContext
#
# # .config('spark.executor.memory', '5gb') \
# # .config("spark.cores.max", "6") \
# spark = SparkSession.builder \
#     .master('local') \
#     .appName('myAppName') \
#     .getOrCreate()
#
# sc = spark.sparkContext

# using SQLContext to read parquet file
# from pyspark.sql import SQLContext
# sqlContext = SQLContext(sc)


def setup(bucket_and_path):
    df = spark.read.parquet(bucket_and_path)
    df.show()
    rdd = df.rdd.map(list)
    # rdd.take(1)
    return rdd, df

# no return just show
def data_cleaning(rdd):
    def clean_tokenize_and_split(s):
        if not s:
            return "<NULL>"
        for blob in text_to_remove:
            s = s.replace(blob, "")
        return s.split(" ")
    review_column_id = 12
    words = rdd.map(lambda row: row[review_column_id])
    words.take(5)
    text_to_remove = [
        "<br />",
        "&#34;",
        "&#34;",
        "\\\\",
    ]
    counts = words.flatMap(lambda line: clean_tokenize_and_split(line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.take(5)
    sorted_counts = counts.sortBy(lambda a: -a[1])
    sorted_counts.take(40)


def explore(df, query):
    # this line registers a "table" with spark sql
    df.createOrReplaceTempView("ratings")
    result = spark.sql(query)
    result.show()
    return result

def dataframe_to_list(df_in):
    # transform the data from a dataframe to a normal python list
    ids = df_in.rdd.map(lambda r: r.product_id).collect()
    return ids


# broadcast_ids = sc.broadcast(ids)


## Filter dataset by ids

def filter_dataset(rdd, ids):
    # sample data cleaning
    # filtered_rdd = rdd.filter(lambda x: x[3] in broadcast_ids)
    filtered_rdd = rdd.filter(lambda x: x[3] in ids)
    # filtered_rdd.take(5)
    return filtered_rdd


## Recommendation

# What's going on here?
# Product and User IDs are not always numeric - sometimes they include letters too
# ALS expects numeric indices for products and users
# We make the following simplification, though it is far from perfect
# What are the problems with this? What should we do instead?

# re.sub('[^0-9]','','abcd12352342342342343')[-8:]


def clean_data(filtered_rdd):
    customer_id = 1
    product_id = 3
    rating_id = 6
    ratingsRDD = filtered_rdd.map(lambda p: Row(userId=int(re.sub('[^0-9]', '', p[customer_id])[-7:]),
                                                productID=int(re.sub('[^0-9]', '', p[product_id])[-7:]),
                                                rating=float(p[rating_id])))
    ratings = spark.createDataFrame(ratingsRDD)
    return ratings


def split_data(ratings):
    (training, testing) = ratings.randomSplit([0.8, 0.2])
    return training, testing


def main():
    input_bucket = 's3://amazon-reviews-pds'
    input_path = '/parquet/product_category=Books/*.parquet'
    input_path = '/parquet/product_category=Books/part-00004-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet'
    input_joined = input_bucket + input_path
    # input_bucket = 's3://amazon-reviews-pds/parquet/product_category=Books/'
    # input_path = '*.parquet'
    # input_joined = input_bucket + input_path
    rdd, df = setup(bucket_and_path=input_joined)
    data_cleaning(rdd=rdd)
    low_ratings_query = """SELECT count(*) as c
            FROM ratings
            WHERE star_rating < 4.0
            Order by c desc"""
    high_ratings_query = """SELECT product_title, AVG(star_rating), count(*) as c
            FROM ratings
            GROUP BY product_title
            HAVING AVG(star_rating) > 4.8 and count(*) > 20
            Order by c desc"""
    high_ratings_by_product_id_query = """SELECT product_id, AVG(star_rating), count(*) as c
            FROM ratings
            GROUP BY product_id
            HAVING AVG(star_rating) > 4.8 and count(*) > 20
            Order by c desc"""
    queries = [low_ratings_query, high_ratings_query, high_ratings_by_product_id_query]
    df_in = None
    for query in queries:
        df_in = explore(df=df, query=query)
    # we are only taking the last result
    result = dataframe_to_list(df_in=df_in)
    # ids = dataframe_to_list(result)
    filtered_rdd = filter_dataset(rdd=rdd, ids=result)
    ratings = clean_data(filtered_rdd=filtered_rdd)
    training, testing = split_data(ratings)
    return training, testing


if __name__ == "__main__":
    output_folder = os.getcwd()
    training, testing = main()

    # DataFrames can be saved as Parquet files, maintaining the schema information.
    # training.write.parquet(output_folder + "/training.parquet")
    training.write.format("parquet").mode("overwrite").save(output_folder + "/training.parquet")
    # DataFrames can be saved as Parquet files, maintaining the schema information.
    # testing.write.parquet(output_folder + "/testing.parquet")
    testing.write.format("parquet").mode("overwrite").save(output_folder + "/testing.parquet")

