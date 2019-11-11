from pyspark.sql import functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, json
from pyspark.sql import Row
import re

# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
import os

import sys


def build_model(training):
    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="productID", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)
    return model, als


if __name__ == "__main__":
    # sc = SparkContext.getOrCreate()
    # spark = SparkSession(sc)

    # Read in the Parquet file created above.
    # Parquet files are self-describing so the schema is preserved.
    # The result of loading a parquet file is also a DataFrame.
    data_path = os.getcwd()
    model_save_path = os.getcwd()

    training = spark.read.parquet(data_path + "/training.parquet")
    model, als = build_model(training=training)

    # this may cause an issue as I have not yet created the path
    als_path = model_save_path + "als/"
    model_path = model_save_path + "als_model/"
    als.write().overwrite().save(als_path)
    model.write().overwrite().save(model_path)
    # als.save(als_path)
    # model.save(model_path)


