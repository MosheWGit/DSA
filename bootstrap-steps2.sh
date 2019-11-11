#!/bin/bash
sudo pip install pandas
sudo yum install tmux -y
sudo pip install apache-airflow[hive]
airflow initdb

