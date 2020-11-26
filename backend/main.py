#!usr/bin/python3

# Standard Imports
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as tp

import json
import os
import socket
import sys
import threading
# Custom Imports
from utils.helper import *

def splitrdd(rdd):
	return rdd

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
	])

	# Teams Schema
	Teams_schema = tp.StructType([
	tp.StructField(name= 'name', 			dataType= tp.StringType(),  nullable= False),
	tp.StructField(name= 'Id', 				dataType= tp.IntegerType(),  nullable= False)
	])
	
	#Metrics storage
	Metrics_schema = tp.StructType([
	tp.StructField(name= 'Id', 				dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'normalPasses', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'keyPasses', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'accNormalPasses', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'accKeyPasses', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'passAccuracy', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'duelsWon', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'neutralDuels', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'totalDuels', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'duelEffectiveness', 		dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'effectiveFreeKicks', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'penaltiesScored', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'totalFreeKicks', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'freeKick', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'targetAndGoal', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'targetNotGoal', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'totalShots', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'shotsOnTarget', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'shotsEffectiveness', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'foulLoss', 	dataType= tp.IntegerType(),  nullable= False),
	tp.StructField(name= 'ownGoals', 	dataType= tp.IntegerType(),  nullable= False)
	])
	
	# Load the Players and Teams data from CSV file
	Player_RDD = ssc.read.csv(Player_CSV_Path, schema=Players_schema, header=True)
	Metrics_RDD= ssc.read.csv(Player_CSV_Path, schema=Metrics_schema,header=True)
	Teams_RDD = ssc.read.csv(Teams_CSV_Path, schema=Teams_schema, header=True)

	# Print the head
	print(Player_RDD.show(5))
	print(Teams_RDD.show(5))
	print(Metrics_RDD.show(5))

	# Initialize the metrics
	# TO-DO


	# Read the streamed data
	strc = StreamingContext(sc, 5)

	# Create an input DStream that will connect to hostname:port, like localhost:9999
	lines = strc.socketTextStream("localhost", 6100)

	# Print InputStream Data
	#lines.pprint()

	# Structure the data and extract relevant data
	rdd=lines.foreach(splitrdd)
	# Compute  the Metrics
	# To-DO


	# Start the computation & Wait for the computation to terminate
	strc.start()
	strc.awaitTermination()  
	strc.stop(stopSparkContext=False, stopGraceFully=True)

