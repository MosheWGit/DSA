#!/bin/bash
sudo yum install tmux -y
sudo pip install apache-airflow[hive]
airflow initdb

