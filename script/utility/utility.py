from airflow.models import Variable
import json
import mysql.connector
from mysql.connector import pooling
import time
import os
from datetime import datetime
import boto3
import configparser
import sys
import pytz
from functools import lru_cache


env = "prod"
config_parser = configparser.ConfigParser()

config_parser.read('/airflow/scripts/cdl_common_{env}.param'.format(env=env))

params_map = {}


def get_param(first_tag, second_tag):
    if params_map.get(first_tag + second_tag) is not None:
        return params_map.get(first_tag + second_tag)
    cdl_param = config_parser[first_tag][second_tag].strip()
    if cdl_param is None:
        config_parser.read('/app/airflow/scripts/cdl_common_{env}.param'.format(env=env))
        cdl_param = config_parser[first_tag][second_tag].strip()
    params_map[first_tag + second_tag] = cdl_param
    return cdl_param
