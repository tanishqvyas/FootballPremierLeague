#!usr/bin/python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
#import listener
from utils.helper import *
import socket,sys,json,threading

players=None
teams=None
def read_data():
	global players
	global teams
	sc = SparkContext("local", "FPL2")
	sql_context = SQLContext(sc)

	players = sql_context.read.csv("../data/players.csv", header=True)
	teams = sql_context.read.csv("../data/teams.csv", header=True)
	
	'''
	for i in df1.collect():
		print(i)
	'''
	#sc.stop()
	#sql_context.stop()
lines=None
def stream_data():
	global lines
	# Create a local StreamingContext with two working thread and batch interval of 1 second
	sc = SparkContext(master="local[2]", appName="FantasyLeagueAnalysis")
	ssc = StreamingContext(sc, 5)

	# Create an input DStream that will connect to hostname:port, like localhost:9999
	lines = ssc.socketTextStream("localhost", 6100)
	calc_metrics(lines)
	#lines.pprint()
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
	read_data()
	stream_data()
	#t1=threading.Thread(target=stream_data)
	#t2=threading.Thread(target=calc_metrics)
	#t1.start()
	#t2.start()
