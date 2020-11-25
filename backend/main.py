#!usr/bin/python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as tp
import os

#import listener
from utils.helper import *
import socket,sys,json,threading


def stream_data():

	# Create a local StreamingContext with two working thread and batch interval of 1 second
	sc = SparkContext(master="local[2]", appName="FantasyLeagueAnalysis")
	ssc = StreamingContext(sc, 5)

	# Create an input DStream that will connect to hostname:port, like localhost:9999
	lines = ssc.socketTextStream("localhost", 6100)
	calc_metrics(lines)

	# Start the computation
	ssc.start()
	# Wait for the computation to terminate
	ssc.awaitTermination()  

	ssc.stop(stopSparkContext=False, stopGraceFully=True)


def calc_metrics(lines):
	#global lines
	sc = SparkContext(master="local", appName="FPL3")
	match=lines.first()
	#events=
	print(match)
	#print(events)



if __name__ =="__main__":

	# Data Paths
	Player_CSV_Path = os.path.join("..", "data", "players.csv")
	Teams_CSV_Path = os.path.join("..", "data", "teams.csv")

	# Initializing spark session : Unified entry point
	sc = SparkContext(appName="FantasyPremierLeague").getOrCreate()
	ssc = SparkSession(sc)

	'''
	Note that the order of columns in the Schema must match that of the columns in the CSV file to be loaded.
	Otherwise the schema gets incorrectly filled or doesn't get filled at places.
	The following schemas have been initialized keeping the above thing in mind.
	'''
	# Player schema
	Players_schema = tp.StructType([
	tp.StructField(name= 'name',   			dataType= tp.StringType(),   nullable= False),
	tp.StructField(name= 'birthArea',   	dataType= tp.StringType(),   nullable= False),
	tp.StructField(name= 'birthDate',   	dataType= tp.TimestampType(),   nullable= False),
	tp.StructField(name= 'foot',   			dataType= tp.StringType(),   nullable= False),
	tp.StructField(name= 'role',   			dataType= tp.StringType(),   nullable= False),
	tp.StructField(name= 'height',   		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'passportArea',   	dataType= tp.StringType(),   nullable= False),
	tp.StructField(name= 'weight',   		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'Id', 				dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'numFouls', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'numGoals', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'numOwnGoals', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'passAccuracy', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'shotsOnTarget', 	dataType= tp.IntegerType(),  nullable= False)
	])

	# Teams Schema
	Teams_schema = tp.StructType([
	tp.StructField(name= 'name', 			dataType= tp.StringType(),  nullable= False),
	tp.StructField(name= 'Id', 				dataType= tp.IntegerType(),  nullable= False)
	])

	# Load the Players and Teams data from CSV file
	Player_RDD = ssc.read.csv(Player_CSV_Path, schema=Players_schema, header=True)
	Teams_RDD = ssc.read.csv(Teams_CSV_Path, schema=Teams_schema, header=True)

	# Print the head
	print(Player_RDD.show(5))
	print(Teams_RDD.show(5))


	# Initialize the metrics
	# TO-DO


	# Read the streamed data
	strc = StreamingContext(sc, 5)

	# Create an input DStream that will connect to hostname:port, like localhost:9999
	lines = strc.socketTextStream("localhost", 6100)

	# Print InputStream Data
	lines.pprint()

	# Structure the data and extract relevant data
	

	# Compute  the Metrics
	# To-DO


	# Start the computation & Wait for the computation to terminate
	strc.start()
	strc.awaitTermination()  
	strc.stop(stopSparkContext=False, stopGraceFully=True)

