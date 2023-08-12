#libraries to install
#pip install colab_env -qU sshtunnel pymysql sqlalchemy==1.4.46 MySQLclient

from google.colab import drive
drive.mount('/content/drive')

import colab_env
#from colab_env import envvar_handler #use to create enviroment variables colab_env.envvar_handler.add_env("VAR_NAME", "VALUE", overwrite=True)
import os
import sshtunnel
import logging
import pymysql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, VARCHAR, String, DateTime, MetaData
import MySQLdb
