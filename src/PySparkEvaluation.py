from pyspark.sql import functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import Row
import re
import sys

# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
import os


def evaluate_model(model, testing):
    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(testing)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))


def generate_predictions_for_all(model):
    # Generate top 10 product recommendations for each user
    userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    movieRecs = model.recommendForAllItems(10)
    return userRecs, movieRecs


def generate_prediction_group(als, model, ratings):
    # Generate top 3 book recommendations for a specified set of users
    users = ratings.select(als.getUserCol()).distinct().limit(1)
    userSubsetRecs = model.recommendForUserSubset(users, 3)

    # Generate top 3 user recommendations for a specified set of books
    books = ratings.select(als.getItemCol()).distinct().limit(3)
    booksSubSetRecs = model.recommendForItemSubset(books, 10)

    return userSubsetRecs, booksSubSetRecs


def main():
    # sc = SparkContext.getOrCreate()
    # spark = SparkSession(sc)

    data_path = os.getcwd()
    training_data_path = data_path + "/training.parquet"
    testing_data_path = data_path + "/testing.parquet"

    training_data = spark.read.parquet(training_data_path)
    testing_data = spark.read.parquet(testing_data_path)

    ratings = training_data.union(testing_data)

    model_save_folder = os.getcwd()
    als_path = model_save_folder + "/als"
    als_model_path = model_save_folder + "/als_model"

    als_module = ALS.load(als_path)
    model = ALSModel.load(als_model_path)

    # Run the tests
    evaluate_model(model=model, testing=testing_data)

    userRecs, movieRecs = generate_predictions_for_all(model=model)

    print(userRecs.head())
    print(movieRecs.head())

    userSubsetRecs, booksSubSetRecs = generate_prediction_group(als=als_module, model=model, ratings=ratings)

    print(userSubsetRecs.take(2))
    print(booksSubSetRecs.take(2))
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Finished!!!!!")
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")


if __name__ == "__main__":
    main()
