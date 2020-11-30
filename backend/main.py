#!usr/bin/python3

# Standard Imports
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.types as tp
from pyspark.sql.functions import lit
import json
import os
import socket
import sys
import threading
from threading import Thread

# Custom Imports
from utils.helper import *
from User_Interface import start_user_service

#test

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
tp.StructField(name= 'previousRating', 		dataType= tp.FloatType(),  nullable= False)
])


# Teams Schema
Teams_schema = tp.StructType([
tp.StructField(name= 'name', 	dataType= tp.StringType(),  nullable= False),
tp.StructField(name= 'Id', 		dataType= tp.IntegerType(),  nullable= False)
])

# Load the Players and Teams data from CSV file
Player_RDD = ssc.read.csv(Player_CSV_Path, schema=Players_schema, header=True)
Teams_RDD = ssc.read.csv(Teams_CSV_Path, schema=Teams_schema, header=True)

sql.registerDataFrameAsTable(Players_RDD, "Player")
sql.registerDataFrameAsTable(Teams_RDD, "Teams")

# initializing player metrics
for i in ['numFouls','numGoals','numOwnGoals','passAcc','shotsOnTarget','normalPasses','keyPasses','accNormalPasses','accKeyPasses']:
	Player_RDD=Player_RDD.withColumn(i,lit(0))
Player_RDD=Player_RDD.withColumn("rating",lit(0.5))
Player_RDD=Player_RDD.withColumn("previousRating",lit(0.5))

print(Player_RDD.show(5))

#Creating metrics of the per match as a whole dataframe
metric_cols=['Id','normalPasses', 'keyPasses','accNormalPasses', 'accKeyPasses','passAccuracy','duelsWon', 'neutralDuels','totalDuels', 'duelEffectiveness', 'effectiveFreeKicks', 'penaltiesScored', 'totalFreeKicks','freeKick', 'targetAndGoal', 'targetNotGoal','totalShots', 'shotsOnTarget', 'shotsEffectiveness', 'foulLoss', 'ownGoals','contribution']
df = sql.sql("select Id from Player").collect()
a=[]
for i in df:
	a.append((i[0],0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
Metrics_RDD=ssc.createDataFrame(a, metric_cols)
sql.registerDataFrameAsTable(Metrics_RDD, "Metrics")


#Creating matches dataframe
b=[]
match_cols=['status','date','label','duration','winner','venue','goals','own_goals','yellow_cards','red_cards']
b.append(('','','','',0,'',0,0,0,0))
Matches_RDD=ssc.createDataFrame(b, cols)
sql.registerDataFrameAsTable(Matches_RDD, "Matches")

#Creating and Initializing player chemistry
player_ids=(Player_RDD.select('Id')).collect()
columns=['player1','player2','chemistry']

player_chemistry = spark.createDataFrame([player_ids[0][0], player_ids[1][0], 0.5], columns)
for j in range(i+2,len(player_ids)):
	newRow = spark.createDataFrame([player_ids[0][0],player_ids[i][0], 0.5],columns)
	player_chemistry = player_chemistry.union(newRow)
	
for i in range(1,len(player_ids)):
	for j in range(i+1,len(player_ids)):
		newRow = spark.createDataFrame([player_ids[i][0],player_ids[j][0], 0.5],columns)
	player_chemistry = player_chemistry.union(newRow)


#Same teams chemistry:
def same_team_chem(player1,player2):
	global player_chemistry
	prev1=Player_RDD.select("previousRating",F.when(F.col("Id")==player1)
	prev2=Player_RDD.select("previousRating",F.when(F.col("Id")==player2)
	r1=Player_RDD.select("rating",F.when(F.col("Id")==player1)
	r2=Player_RDD.select("rating",F.when(F.col("Id")==player2)

	change=abs(((r1-prev1)+(r2-prev2))/2)
	if((r1-prev1) <0 and (r2-prev2) <0) or ((r1-prev1)>0 and (r2-prev2)>0):
		sign=1
	else:
		sign=-1

	df1 = player_chemistry.filter(player_chemistry.player1.contains(player1))
	df1 = df1.filter(df1.player2.contains(player2))

	if(df1==None):
		df1 = player_chemistry.filter(player_chemistry.player1.contains(player2))
		df1 = df1.filter(df1.player2.contains(player1))
		row=df1.collect()
		to_insert=df1[2]+(sign*change)
		player_chemistry=player_chemistry.withColumn("chemistry",F.when(F.col("player1")==player2,F.col("player2")==player1,to_insert).otherwise(F.col("chemistry")))
	else:
		row=df1.collect()
		to_insert=df1[2]+(sign*change)
		player_chemistry=player_chemistry.withColumn("chemistry",F.when(F.col("player1")==player1,F.col("player2")==player2,to_insert).otherwise(F.col("chemistry")))


def opposite_team_chem(player1,player2):
	global player_chemistry
	prev1=Player_RDD.select("previousRating",F.when(F.col("Id")==player1)
	prev2=Player_RDD.select("previousRating",F.when(F.col("Id")==player2)
	r1=Player_RDD.select("rating",F.when(F.col("Id")==player1)
	r2=Player_RDD.select("rating",F.when(F.col("Id")==player2)

	change=abs(((r1-prev1)+(r2-prev2))/2)
	if((r1-prev1) <0 and (r2-prev2) <0) or ((r1-prev1)>0 and (r2-prev2)>0):
		sign=-1
	else:
		sign=1

	df1 = player_chemistry.filter(player_chemistry.player1.contains(player1))
	df1 = df1.filter(df1.player2.contains(player2))

	if(df1==None):
		df1 = player_chemistry.filter(player_chemistry.player1.contains(player2))
		df1 = df1.filter(df1.player2.contains(player1))
		row=df1.collect()
		to_insert=df1[2]+(sign*change)
		player_chemistry=player_chemistry.withColumn("chemistry",F.when(F.col("player1")==player2,F.col("player2")==player1,to_insert).otherwise(F.col("chemistry")))
	else:
		row=df1.collect()
		to_insert=df1[2]+(sign*change)
		player_chemistry=player_chemistry.withColumn("chemistry",F.when(F.col("player1")==player1,F.col("player2")==player2,to_insert).otherwise(F.col("chemistry")))
	
def calc_contrib_and_rating(i,stored):
	global Player_RDD
	global Metrics_RDD
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

	for j in bench:
		Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j,0).otherwise(F.col("contribution")))
		#df2=Metrics_RDD.filter(Metrics_RDD.Id == j)
		#print(df2.collect()[0])
	for j in lineup:
		df2=Metrics_RDD.filter(Metrics_RDD.Id == j)
		values=df2.collect()[0]
		contrib=1.05*get_player_contribution(values[5], values[9],values[13],values[17])
		Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j,contrib).otherwise(F.col("contribution")))
		foul=Player_RDD.select("numFouls",F.when(F.col("Id")==j))
		own_goal=Player_RDD.select("ownGoals",F.when(F.col("Id")==j))
		playerPerformance=contrib*pow(0.995,foul)*pow(0.95,own_goal)
		player_prev_rating=Player_RDD.select("rating",F.when(F.col("Id")==j))
		Player_RDD=Player_RDD.withColumn("previousRating",F.when(F.col("Id")==j,(player_rating_prev)).otherwise(F.col("previousRating")))
		Player_RDD=Player_RDD.withColumn("rating",F.when(F.col("Id")==j,((playerPerformance+player_prev_rating)/2)).otherwise(F.col("rating")))
	
	for j in playedtime:
		df2=Metrics_RDD.filter(Metrics_RDD.Id == j[0])
		values=df2.collect()[0]
		contrib=j[1]*get_player_contribution(values[5], values[9],values[13],values[17])
		Metrics_RDD=Metrics_RDD.withColumn("contribution",F.when(F.col("Id")==j[0],contrib).otherwise(F.col("contribution")))
		foul=Player_RDD.select("numFouls",F.when(F.col("Id")==j))
		own_goal=Player_RDD.select("ownGoals",F.when(F.col("Id")==j))
		playerPerformance=contrib*pow(0.995,foul)*pow(0.95,own_goal)
		player_prev_rating=Player_RDD.select("rating",F.when(F.col("Id")==j))
		Player_RDD=Player_RDD.withColumn("previousRating",F.when(F.col("Id")==j,(player_rating_prev)).otherwise(F.col("previousRating")))
		Player_RDD=Player_RDD.withColumn("rating",F.when(F.col("Id")==j,((playerPerformance+player_prev_rating)/2)).otherwise(F.col("rating")))
	return bench,lineup,substitutions


def insert_into_matches(stored):
	global match_cols
	global Matches_RDD
	#match_cols=['date','label','duration','winner','venue','goals','own_goals','yellow_cards','red_cards']
	for i in stored["teamsData"]:
	
	newRow = spark.createDataFrame([stored['status'],stored['date'],stored['label'],stored['duration'],stored['winner'],stored['venue'],stored['']],match_cols)
	Matches_RDD= Matches_RDD.union(newRow)
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
	global Player_RDD
	global sql
	#print(rdd,type(rdd))
	rdds=[json.loads(i) for i in rdd.collect()]
	stored=[]
	for data in rdds:
		print(data)
		if 'eventId' in data:
			player=data['playerId']
			df2=Player_RDD.filter(Player_RDD.Id == player)
			if df2.collect():
				values=df2.collect()[0]
				
				df3=Metrics_RDD.filter(Metrics_RDD.Id == player)
				metrics_values=df3.collect()[0]
				
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
					Player_RDD=Player_RDD.withColumn("passAcc",F.when(F.col("Id")==player,to_insert).otherwise(F.col("passAcc")))
					Player_RDD=Player_RDD.withColumn("normalPasses",F.when(F.col("Id")==player,num_normal_passes).otherwise(F.col("normalPasses")))
					Player_RDD=Player_RDD.withColumn("keyPasses",F.when(F.col("Id")==player,num_key_passes).otherwise(F.col("keyPasses")))
					Player_RDD=Player_RDD.withColumn("accNormalPasses",F.when(F.col("Id")==player,num_acc_normal_passes).otherwise(F.col("accNormalPasses")))
					Player_RDD=Player_RDD.withColumn("accKeyPasses",F.when(F.col("Id")==player,num_acc_key_passes).otherwise(F.col("accKeyPasses")))
				
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
						num_effec_free_kicks+=1
					total_free_kicks+=1
					
					
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
					Player_RDD=Player_RDD.withColumn("shotsOnTarget",F.when(F.col("Id")==player,player_shots_on_target).otherwise(F.col("shotsOnTarget")))
					Player_RDD=Player_RDD.withColumn("numGoals",F.when(F.col("Id")==player,goals).otherwise(F.col("numGoals")))
				if x == 2:	#foul
					foul=values[7]	#values[19]	#get from dataframe
					Player_RDD=Player_RDD.withColumn("numFouls",F.when(F.col("Id")==player,(foul+1)).otherwise(F.col("numFouls")))
					permatch_foul=metrics_values[19]
					Metrics_RDD=Metrics_RDD.withColumn("foulLoss",F.when(F.col("Id")==player,(permatch_foul+1)).otherwise(F.col("foulLoss")))
				if 102 in v:	#own goal
					own_goals=values[9]	#get from dataframe
					Player_RDD=Player_RDD.withColumn("numOwnGoals",F.when(F.col("Id")==player,(own_goals+1)).otherwise(F.col("numOwnGoals")))
					permatch_own_goals=metrics_values[20]
					Metrics_RDD=Metrics_RDD.withColumn("ownGoals",F.when(F.col("Id")==player,(permatch_own_goals+1)).otherwise(F.col("ownGoals")))
					
					
				#checking Metrics per match and player profiles updated	
				#df2=Metrics_RDD.filter(Metrics_RDD.Id == player)
				#print(df2.collect()[0])
				'''
				df2=Player_RDD.filter(Player_RDD.Id == player)
				print(df2.collect()[0])
				'''
		else:
			
			#its match data dict
			print("match data")
			#INSERT INTO Matches_RDD
			
			
			#calculating previous match data after the events
			if stored and stored['status']=='Played':
				#match is over # OR IS THERE SOME OTHER VALUE
				ids=[]
				count=1
				bench1=[]
				bench2=[]
				lineup1=[]
				lineup2=[]
				substitutions1=[]
				substitutions2=[]
				
				for i in stored['teamsData'] and count<=2:
					#i is the teamID
					ids.append(i)
					if stored['teamsData'][i]['hasformation']==1:
						if c==1:
							bench1,lineup1,substitutions1=calc_contrib_and_rating(i,stored)
						if c==2:
							bench2,lineup2,substitutions2=calc_contrib_and_rating(i,stored)
						count+=1
					if c==3:

						# Calculating ratings after reading both the players
						substitutions1=[i['playerIn'] for i in substitutions1]
						substitutions2=[i['playerIn'] for i in substitutions2]
						for i in lineup1+substitutions1:
							for j in lineup2+substitutions2:
								opposite_team_chem(i,j)
							for k in lineup1+substitutions2:
								if(i!=k):
									same_team_chem(i,k)
						
						for i in lineup2+substitutions2:
							for k in lineup2+substitutions2:
								if(i!=k):
									same_team_chem(i,k)



			# new match data
			stored=data
			
			insert_into_matches(stored)
			
			if stored['status']!='Played':
				break
			#initializing the values of all the metrics
			global metric_cols
			for i in metric_cols[1:]:
				Metrics_RDD=Metrics_RDD.withColumn(i,lit(0))


# Runnning the User CLI as a separate Thread
thread = Thread(target = start_user_service, args=(Metrics_RDD, Players_RDD))
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
