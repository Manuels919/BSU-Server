#Libraries to install
#pip install colab_env -qU sshtunnel pymysql sqlalchemy==1.4.46 MySQLclient

#colab_env: to access and edit enviromental variables.
#sshtunnel: allows you to create a connection to an ssh server using python.
#MySQLclient: grants access to all of the tools desigined to connect to MySQL Server.
#pymysql: allows you to perform CRUD (Create, Read, Update, Delete) functions to manage the database.
#sqlachemy: facilitates the communication between Python programs and databases.

#Code was made for google drive but can easily be changed
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

#
#
#Server Variables
SERVER_IP  = os.getenv('SERVER_IP')
SERVER_PORT = int(os.getenv('SERVER_PORT')) # or ip
SERVER_USER = os.getenv('SERVER_USER')
SERVER_USER_PASSWORD  = os.getenv('SERVER_USER_PASSWORD')

#Database Variables
SQL_SERVER_USER  = os.getenv('SQL_SERVER_USER')
SQL_SERVER_USER_PASSWORD = os.getenv('SQL_SERVER_USER_PASSWORD')
SQL_DB_NAME  = os.getenv('SQL_DB_NAME')
SQL_DP_PORT = int(os.getenv('SQL_DB_PORT'))
LOCAL_HOST = os.getenv('Local_Host')

#
#
#Functions 

#Create the connection to the server
def serverConnection(verbose=False): 

  if verbose:
    sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

  global tunnel
  #connects to the server and creates a tunnel connection
  tunnel = SSHTunnelForwarder((SERVER_IP, SERVER_PORT), 
                                  ssh_username= SERVER_USER, ssh_password=SERVER_USER_PASSWORD,
                                  remote_bind_address=(LOCAL_HOST, SQL_DP_PORT))
  
  tunnel.start()


#Establish a connection to the database using PyMySQL
def sqlConnection():

  global connection

  connection = pymysql.connect(host=LOCAL_HOST, user=SQL_SERVER_USER,
                               passwd=SQL_SERVER_USER_PASSWORD, db=SQL_DB_NAME, 
                               port=tunnel.local_bind_port)


#Takes a SQL query as a string and returns a dataframe using pandas.
def runQueryPyMySQL(sql):
  #returns a panda dataframe of results from query
  return pd.read_sql_query(sql, connection)


#Creates a connection to the DB using SQLAlchemy instead of PyMySQL
#Instead of only being able to return a dataframe. This function can execute any SQL statement.
def runQuerySQLAlchemy(sql=None):
  global engine
  local_port = str(tunnel.local_bind_port)
  engine = sqlalchemy.create_engine(
  f'mysql+mysqldb://{SQL_SERVER_USER}:{SQL_SERVER_USER_PASSWORD}@{LOCAL_HOST}:{local_port}/{SQL_DB_NAME}')

  if sql == None:
    pass
  else:
    return engine.execute(sql)

#Closes the connection to the SQL Database
def closeSQLConnection():
  connection.close() 

#Closes the connection to the server
def closeServerConnection():
  tunnel.close 

#
#
#Data Upload

serverConnection() #Creates server connection
sqlConnection() #Creates database connection using PyMySQL
runQuerySQLAlchemy() #Creates database connection using SQLAlchemy engine

##Inititalizes metadata object that stores the information of the table or any object that references it
metadata_obj = MetaData()

#Table Creation
#
# #When creating tables be mindful that column names must match the the column of the dataframe
# Taskstream = Table(
#     "Taskstream", metadata_obj,
#     Column("StudentID", String(16), nullable=False),
#     Column("ProgramName", String(255)),
#     Column("Name", String(255)),
#     Column("CriteriaName", VARCHAR(1050)),
#     Column("RubricRowPoints", Float()),
#     Column("timestamp", DateTime()),
# )

# Completers = Table(
#     "Completers", metadata_obj,
#     Column("BANNER_ID", String(16), nullable=False),
#     Column("DEGREE_MAJORid", String(20)),
#     Column("GRAD_TERM", Integer()),
#     Column("COHORT", String(25)),
#     Column("AdvInit", String(15)),
#     Column("ProgGroup", String(255)),
#     Column("ProgAbbrev", String(50)),
#     Column("ProgLevel", String(5)),
# )

# uniqueCompleters = Table(
#     "Students", metadata_obj,
#     Column("BANNER_ID", String(16), nullable=False, primary_key=True),
#     Column("ETHNICITY", String(20)),
#     Column("GENDER", String(16)),
# )

##The engine executes all of the objects that are stored within the metadata object
metadata_obj.create_all(engine)

#
#Use pandas to read the files and assign them to dataframes

path = os.getenv('folder_path')
taskstreamDF = pd.read_csv(path + 'Taskstream.csv')
completersDF = pd.read_csv(path + '221026completers.csv')
uniqueCompletersDF = pd.read_csv(path + 'uniqueCompleters.csv')

## To repalce data in table use code below
# DATAFRAME.to_sql("TABLE NAME", engine, if_exists="replace", index=False)

#If adding data to existing database use "append"
uniqueCompletersDF.to_sql("Students", engine, if_exists="append", index=False)
taskstreamDF.to_sql("Taskstream", engine, if_exists="append", index=False)
completersDF.to_sql("Completers", engine, if_exists="append", index=False)


#
#To read the data you just uploaded in the tables
runQueryPyMySql("SELECT * FROM Completers WHERE COHORT == 2021")


closeSQLConnection() #Only needs to be used when using runQueryPyMySQL() or when reading the table directly using pandas
closeServerConnection() #Closes the connection to the server





