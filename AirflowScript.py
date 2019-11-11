# airflowRedditPysparkDag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

'''
input arguments for downloading S3 data 
and Spark jobs
REMARK: 
Replace `srcDir` and `redditFile` as the full paths containing your PySpark scripts
and location of the Reddit file will be stored respectively 
'''
# s3Bucket = '<YOUR_S3_BUCKET>'
# s3Key = '<YOUR_S3_KEY>'
# cleaned_data = os.getcwd() + '/data/'
# model_path = os.getcwd() + '/model/'
# srcDir = os.getcwd() + '/src/'
s3 = 's3://applieddatascience/'
cleaned_data = s3 + 'data/'
model_path = s3 + 'model/'
srcDir = s3 + 'src/'

sparkSubmit = '/usr/local/spark/bin/spark-submit'
sparkSubmit = 'spark-submit'

## Define the DAG object
default_args = {
    'owner': 'weinreb-moshe',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# last part should schedule every thirty minutes
dag = DAG('DSAPySparkDemo', default_args=default_args, schedule_interval=timedelta(hours=1))

'''
Defining three tasks: one task to download S3 data
and two Spark jobs that depend on the data to be 
successfully downloaded
task to download data
'''
preprocessData = BashOperator(
    task_id='preprocess-data',
    bash_command=sparkSubmit + ' ' + srcDir + 'PySparkPreProcessing.py',
    dag=dag)

# task to compute number of unique authors
generateModel = BashOperator(
    task_id='generate-model',
    bash_command=sparkSubmit + ' ' + srcDir + 'PySparkModel.py',
    dag=dag)

# Specify that this task depends on the downloadData task
generateModel.set_upstream(preprocessData)

# task to compute average upvotes
seeSugesstions = BashOperator(
    task_id='evaluate-model',
    bash_command=sparkSubmit + ' ' + srcDir + 'PySparkEvaluation.py',
    dag=dag)
seeSugesstions.set_upstream(generateModel)
