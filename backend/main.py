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



# Data Paths
Player_CSV_Path = os.path.join("..", "data", "players.csv")
Teams_CSV_Path = os.path.join("..", "data", "teams.csv")

# Initializing spark session : Unified entry point
sc = SparkContext(master="local[2]",appName="FantasyPremierLeague").getOrCreate()
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
#print(Player_RDD.show(5))
#print(Teams_RDD.show(5))
#print(Metrics_RDD.show(5))


def calc_metrics(rdd):
	global Metrics_RDD
	#print(rdd,type(rdd))
	rdds=[json.loads(i) for i in rdd.collect()]
	for data in rdds:
		try:
			if 'eventId' in data:
				x=data['eventId']
				v=[j['id'] for j in data['tags']]
				
				if x == 8:	#Pass
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0
					num_acc_normal_passes=0
					num_acc_key_passes=0
					num_normal_passes=0
					num_key_passes=0
					if 1801 in v:
						#accurate pass
						if 302 in v:
							num_acc_key_passes+=1
							num_key_passes+=1
						else:
							num_acc_normal_passes+=1
							num_normal_passes+=1
					elif 1802 in v:
						# not accurate pass
						num_normal_passes+=1
					#is there anything like not accurate key pass
					
					elif 302 in v:
						# key pass
						num_key_passes+=1
					to_insert=get_pass_accuracy(num_acc_normal_passes, num_acc_key_passes, num_normal_passes, num_key_passes)
					# insert this into the passaccuracy column of the Metrics_RDD
				
				if x == 1:	#duels
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0
					num_duels_won=0
					num_neutral_duels=0
					total_duels=0
					if 701 in v:
						#lost
						total_duels+=1
					elif 702 in v:
						# neutral
						num_neutral_duels+=1
						total_duels+=1
					elif 703 in v:
						# won
						num_duels_won+=1
						total_duels+=1
					to_insert=get_duel_effectiveness(num_duels_won, num_neutral_duels, total_duels)
					# insert this into the duel effectiveness column of the Metrics_RDD
				
				if x == 3:	#free kick
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0
					
					num_penalties_scored=0
					num_effec_free_kicks=0
					total_free_kicks=0
					
					if data['subEventId'] ==35 and 101 in v:
						num_penalties_scored+=1
					
					#FIND DIFFERENCE BETWEEN EFFECTIVE AND GOAL PENALTY WUT 
					if 1801 in v:
						#effective
						total_free_kicks+=1
					elif 1802 in v:
						# not effective
						num_effec_free_kicks+=1
						total_free_kicks+=1
					to_insert=get_freekick_effectiveness(num_effec_free_kicks, num_penalties_scored, total_free_kicks)
					# insert this into the free kick effectiveness column of the Metrics_RDD
		except:
			#its match data dict
			print("match data")



# Read the streamed data
strc = StreamingContext(sc, 5)

# Create an input DStream that will connect to hostname:port, like localhost:9999
lines = strc.socketTextStream("localhost", 6100)

# Print InputStream Data

#lines.pprint()
# Structure the data and extract relevant data
#data=lines.map(lambda line:json.loads(line))
lines.foreachRDD(calc_metrics)

#data=lines.map(lambda line:json.loads(line))
#data.pprint()
#lines=lines.map(lambda line:calc_metrics(line))
#lines.pprint()
# Compute  the Metrics
# To-DO


# Start the computation & Wait for the computation to terminate
strc.start()



strc.awaitTermination()  
strc.stop(stopSparkContext=False, stopGraceFully=True)

