#!usr/bin/python3

# Standard Imports
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.types as tp

import json
import os
import socket
import sys
import threading
from threading import Thread

# Custom Imports
from utils.helper import *
from User_Interface import start_user_service



# Data Paths
Player_CSV_Path = os.path.join("..", "data", "players.csv")
Teams_CSV_Path = os.path.join("..", "data", "teams.csv")

# Initializing spark session : Unified entry point
sc = SparkContext(master="local[2]",appName="FantasyPremierLeague").getOrCreate()
ssc = SparkSession(sc)
sql = SQLContext(sc)
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
tp.StructField(name= 'passAcc', 		dataType= tp.FloatType(),  nullable= False),
tp.StructField(name= 'shotsOnTarget', 		dataType= tp.IntegerType(),  nullable= False),
tp.StructField(name= 'normalPasses', 		dataType= tp.IntegerType(),  nullable= False),
tp.StructField(name= 'keyPasses', 		dataType= tp.IntegerType(),  nullable= False),
tp.StructField(name= 'accNormalPasses', 		dataType= tp.IntegerType(),  nullable= False),
tp.StructField(name= 'accKeyPasses', 		dataType= tp.IntegerType(),  nullable= False),
tp.StructField(name= 'rating', 		dataType= tp.FloatType(),  nullable= False)
])


# Teams Schema
Teams_schema = tp.StructType([
tp.StructField(name= 'name', 	dataType= tp.StringType(),  nullable= False),
tp.StructField(name= 'Id', 		dataType= tp.IntegerType(),  nullable= False)
])

# Load the Players and Teams data from CSV file
Players_RDD = ssc.read.csv(Player_CSV_Path, schema=Players_schema, header=True)
Teams_RDD = ssc.read.csv(Teams_CSV_Path, schema=Teams_schema, header=True)

sql.registerDataFrameAsTable(Players_RDD, "Player")
sql.registerDataFrameAsTable(Teams_RDD, "Teams")



#Creating metrics of the per match as a whole dataframe
cols=['Id','normalPasses', 'keyPasses','accNormalPasses', 'accKeyPasses','passAccuracy','duelsWon', 'neutralDuels','totalDuels', 'duelEffectiveness', 'effectiveFreeKicks', 'penaltiesScored', 'totalFreeKicks','freeKick', 'targetAndGoal', 'targetNotGoal','totalShots', 'shotsOnTarget', 'shotsEffectiveness', 'foulLoss', 'ownGoals','contribution']
df = sql.sql("select Id from Player").collect()
a=[]
for i in df:
	Players_RDD=Players_RDD.withColumn("rating",F.when(F.col("Id")==i[0],0.5).otherwise(F.col("rating")))
	a.append((i[0],0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
Metrics_RDD=ssc.createDataFrame(a, cols)
sql.registerDataFrameAsTable(Metrics_RDD, "Metrics")


#Creating matches dataframe
b=[]
cols=['date','label','duration','winner','venue','goals','own_goals','yellow_cards','red_cards']#NEED TO STORE ALL MATCH DETAILS ALSO
b.append((None,None,None,None,None,None,None,None,None))
Matches_RDD=ssc.createDataFrame(b, cols)
sql.registerDataFrameAsTable(Matches_RDD, "Matches")

'''
#TRIAL
player=65880
df2 =Metrics_RDD.filter(Metrics_RDD.Id == player)
print(df2.collect()[0][0])
Metrics_RDD=Metrics_RDD.withColumn("Id",F.when(F.col("Id")==player,1000).otherwise(F.col("Id")))
print(Metrics_RDD.show(5))


Function to process the match and event Jsons
'''
def calc_metrics(rdd):
	global Metrics_RDD
	global Matches_RDD
	global Players_RDD
	global sql
	#print(rdd,type(rdd))
	rdds=[json.loads(i) for i in rdd.collect()]
	for data in rdds:
		if 'playerId' in data:
			player=data['playerId']
			df2=Players_RDD.filter(Players_RDD.Id == player)
			values=df2.collect()[0]
			
			df3=Metrics_RDD.filter(Metrics_RDD.Id == player)
			metrics_values=df3.collect()[0]
			if 'eventId' in data:
				x=data['eventId']
				v=[j['id'] for j in data['tags']]
				
				if x == 8:	#Pass
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0
					num_acc_normal_passes=values[16]
					num_acc_key_passes=values[17]
					num_normal_passes=values[14]
					num_key_passes=values[15]
					
					match_num_acc_normal_passes=metrics_values[3]
					match_num_acc_key_passes=metrics_values[4]
					match_num_normal_passes=metrics_values[1]
					match_num_key_passes=metrics_values[2]
					
					if 1801 in v:
						#accurate pass
						if 302 in v:
							match_num_acc_key_passes+=1
							match_num_key_passes+=1
						else:
							match_num_acc_normal_passes+=1
							match_num_normal_passes+=1
					elif 1802 in v:
						# not accurate pass
						match_num_normal_passes+=1
					#is there anything like not accurate key pass
					
					elif 302 in v:
						# key pass
						match_num_key_passes+=1
					
					#FOR CURRENT MATCH
					per_match_pass_accuracy=get_pass_accuracy(match_num_acc_normal_passes, match_num_acc_key_passes, match_num_normal_passes, match_num_key_passes)
					Metrics_RDD=Metrics_RDD.withColumn("passAccuracy",F.when(F.col("Id")==player,per_match_pass_accuracy).otherwise(F.col("passAccuracy")))
					Metrics_RDD=Metrics_RDD.withColumn("normalPasses",F.when(F.col("Id")==player,match_num_normal_passes).otherwise(F.col("normalPasses")))
					Metrics_RDD=Metrics_RDD.withColumn("keyPasses",F.when(F.col("Id")==player,match_num_key_passes).otherwise(F.col("keyPasses")))
					Metrics_RDD=Metrics_RDD.withColumn("accNormalPasses",F.when(F.col("Id")==player,match_num_acc_normal_passes).otherwise(F.col("accNormalPasses")))
					Metrics_RDD=Metrics_RDD.withColumn("accKeyPasses",F.when(F.col("Id")==player,match_num_acc_key_passes).otherwise(F.col("accKeyPasses")))
				
					#FOR WHOLE PLAYER
					num_acc_normal_passes+=match_num_acc_normal_passes
					num_acc_key_passes+=match_num_acc_key_passes
					num_normal_passes+=match_num_normal_passes
					num_key_passes+=match_num_key_passes
					to_insert=get_pass_accuracy(num_acc_normal_passes, num_acc_key_passes, num_normal_passes, num_key_passes)
					Players_RDD=Players_RDD.withColumn("passAcc",F.when(F.col("Id")==player,to_insert).otherwise(F.col("passAcc")))
					Players_RDD=Players_RDD.withColumn("normalPasses",F.when(F.col("Id")==player,num_normal_passes).otherwise(F.col("normalPasses")))
					Players_RDD=Players_RDD.withColumn("keyPasses",F.when(F.col("Id")==player,num_key_passes).otherwise(F.col("keyPasses")))
					Players_RDD=Players_RDD.withColumn("accNormalPasses",F.when(F.col("Id")==player,num_acc_normal_passes).otherwise(F.col("accNormalPasses")))
					Players_RDD=Players_RDD.withColumn("accKeyPasses",F.when(F.col("Id")==player,num_acc_key_passes).otherwise(F.col("accKeyPasses")))
				
				if x == 1:	#duels
					#get the below values from the dataframe Metrics_RDD
					#MAKE SURE TO RESET THE METRICS RDD EACH TIME
					num_duels_won=metrics_values[6]
					num_neutral_duels=metrics_values[7]
					total_duels=metrics_values[8]
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
					
					
					per_match_duel_effectiveness=get_duel_effectiveness(num_duels_won, num_neutral_duels, total_duels)
					
					# Per_match
					Metrics_RDD=Metrics_RDD.withColumn("duelEffectiveness",F.when(F.col("Id")==player,to_insert).otherwise(F.col("duelEffectiveness")))
					Metrics_RDD=Metrics_RDD.withColumn("totalDuels",F.when(F.col("Id")==player,total_duels).otherwise(F.col("totalDuels")))
					Metrics_RDD=Metrics_RDD.withColumn("neutralDuels",F.when(F.col("Id")==player,num_neutral_duels).otherwise(F.col("neutralDuels")))
					Metrics_RDD=Metrics_RDD.withColumn("duelsWon",F.when(F.col("Id")==player,num_duels_won).otherwise(F.col("duelsWon")))
				
				
				if x == 3:	#free kick
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0
					
					num_effec_free_kicks=metrics_values[10]
					num_penalties_scored=metrics_values[11]
					total_free_kicks=metrics_values[12]
					
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
					
					
					#per_match_freekick_effectiveness=get_freekick_effectiveness(num_effec_free_kicks-values[10], num_penalties_scored-values[11], total_free_kicks-values[12])
					to_insert=get_freekick_effectiveness(num_effec_free_kicks, num_penalties_scored, total_free_kicks)
					
					# insert this into the free kick effectiveness column of the Metrics_RDD
					Metrics_RDD=Metrics_RDD.withColumn("freeKick",F.when(F.col("Id")==player,to_insert).otherwise(F.col("freeKick")))
					Metrics_RDD=Metrics_RDD.withColumn("effectiveFreeKicks",F.when(F.col("Id")==player,num_effec_free_kicks).otherwise(F.col("effectiveFreeKicks")))
					Metrics_RDD=Metrics_RDD.withColumn("penaltiesScored",F.when(F.col("Id")==player,num_penalties_scored).otherwise(F.col("penaltiesScored")))
					Metrics_RDD=Metrics_RDD.withColumn("totalFreeKicks",F.when(F.col("Id")==player,total_free_kicks).otherwise(F.col("totalFreeKicks")))
					
				if x == 10:	#shots effectiveness
					#get the below values from the dataframe Metrics_RDD
					#if None is the value start at 0

					shots_on_trgt_and_goals=metrics_values[14]
					shots_on_trgt_but_not_goals=metrics_values[15]
					total_shots=metrics_values[16]
					shotsOnTarget=metrics_values[17]
					
					player_shots_on_target=values[13]
					goals=values[8]
					#FIND DIFFERENCE BETWEEN EFFECTIVE AND GOAL PENALTY WUT
					if 101 in v:
						goals+=1
					if 1801 in v:
						#on traget
						total_shots+=1
						if 101 in v:
							#goal shot
							shots_on_trgt_and_goals+=1
						else:
							#not goal but target
							shots_on_trgt_but_not_goals+=1
					elif 1802 in v:
						# not on target
						total_shots+=1
						
					
					shotsOnTarget+=shots_on_trgt_and_goals+shots_on_trgt_but_not_goals
					
					to_insert=get_shots_effectiveness(shots_on_trgt_and_goals, shots_on_trgt_but_not_goals, total_shots)
					
					# insert this into the free kick effectiveness column of the Metrics_RDD
					Metrics_RDD=Metrics_RDD.withColumn("shotsEffectiveness",F.when(F.col("Id")==player,to_insert).otherwise(F.col("shotsEffectiveness")))
					Metrics_RDD=Metrics_RDD.withColumn("shotsOnTarget",F.when(F.col("Id")==player,shotsOnTarget).otherwise(F.col("shotsOnTarget")))
					Metrics_RDD=Metrics_RDD.withColumn("totalShots",F.when(F.col("Id")==player,total_shots).otherwise(F.col("totalShots")))
					Metrics_RDD=Metrics_RDD.withColumn("targetNotGoal",F.when(F.col("Id")==player,shots_on_trgt_but_not_goals).otherwise(F.col("targetNotGoal")))
					Metrics_RDD=Metrics_RDD.withColumn("targetAndGoal",F.when(F.col("Id")==player,shots_on_trgt_and_goals).otherwise(F.col("targetAndGoal")))
				
					player_shots_on_target+=shotsOnTarget
					Players_RDD=Players_RDD.withColumn("shotsOnTarget",F.when(F.col("Id")==player,player_shots_on_target).otherwise(F.col("shotsOnTarget")))
					Players_RDD=Players_RDD.withColumn("numGoals",F.when(F.col("Id")==player,goals).otherwise(F.col("numGoals")))
				if x == 2:	#foul
					foul=values[7]	#values[19]	#get from dataframe
					Players_RDD=Players_RDD.withColumn("numFouls",F.when(F.col("Id")==player,(foul+1)).otherwise(F.col("numFouls")))
					permatch_foul=metrics_values[19]
					Metrics_RDD=Metrics_RDD.withColumn("foulLoss",F.when(F.col("Id")==player,(permatch_foul+1)).otherwise(F.col("foulLoss")))
				if 102 in v:	#own goal
					own_goals=values[9]	#get from dataframe
					Players_RDD=Players_RDD.withColumn("numOwnGoals",F.when(F.col("Id")==player,(own_goals+1)).otherwise(F.col("numOwnGoals")))
					permatch_own_goals=metrics_values[20]
					Metrics_RDD=Metrics_RDD.withColumn("ownGoals",F.when(F.col("Id")==player,(permatch_own_goals+1)).otherwise(F.col("ownGoals")))
				
				
				#checking Metrics per match and player profiles updated	
				df2=Metrics_RDD.filter(Metrics_RDD.Id == player)
				print(df2.collect()[0])
				'''
				df2=Players_RDD.filter(Players_RDD.Id == player)
				print(df2.collect()[0])
				'''
		else:
			#its match data dict
			print("match data")
			#INSERT INTO Matches_RDD
			#
			#
			#
			stored=data
	if stored['status']=='Played':
		#match is over # OR IS THERE SOME OTHER VALUE
		
		for i in stored['teamsData']:
			#i is the teamID
			bench=[i['playerId'] for i in stored['teamsData'][i]['formation']['bench']]
			lineup=[i['playerId'] for i in stored['teamsData'][i]['formation']['lineup']]
			substitutions=stored['teamsData'][i]['formation']['substitutions']
			playedtime=[]
			for j in substitutions:	# FIND OUT IF THERE ARE ONLY 3 SUBS PER MATCH FROM SIR
				inplayer=j['playerIn']
				outplayer=j['playerOut']
				minute=j['minute']
				
				if inplayer in bench:#cuz in player cant be in lineup
					#INSERT A METRIC AS TIME PLAYED
					playedtime.append((inplayer,(90-minute)/90.0))
					bench.remove(inplayer)
				if outplayer in lineup:
					playedtime.append((outplayer,(minute)/90.0))
					outplayer.remove(outplayer)
					continue
				for k in len(playedtime):
					if playedtime[k][0]==outplayer:
						playedtime[k][1]=(playedtime[k][1]*90.0)-90+minute
						break
			
			players_who_played=[]
			for j in bench:
				Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j,0).otherwise(F.col("contribution")))
			for j in lineup:
				df2=Metrics_RDD.filter(Metrics_RDD.Id == j)
				values=df2.collect()[0]
				Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j,(1.05*get_player_contribution(values[5], values[9],values[13],values[17]))).otherwise(F.col("contribution")))
			for j in playedtime:
				df2=Metrics_RDD.filter(Metrics_RDD.Id == j[0])
				values=df2.collect()[0]
				Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j[0],(j[1]*get_player_contribution(values[5], values[9],values[13],values[17]))).otherwise(F.col("contribution")))
				
			
			#MUST CALCULATE RATINGS


# Runnning the User CLI as a separate Thread
thread = Thread(target = start_user_service, args=(Metrics_RDD, Player_RDD))
thread.start()


# Read the streamed data
strc = StreamingContext(sc, 5)

# Create an input DStream that will connect to hostname:port, like localhost:9999
lines = strc.socketTextStream("localhost", 6100)

# Print InputStream Data
# lines.pprint()

# Calculate Metrics for all input stream
lines.foreachRDD(calc_metrics)




# Start the computation & Wait for the computation to terminate
strc.start()
strc.awaitTermination()  
strc.stop(stopSparkContext=False, stopGraceFully=True)

