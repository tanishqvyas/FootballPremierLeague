from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(master="local[2]", appName="FantasyLeagueAnalysis")
ssc = StreamingContext(sc, 5)


# Create an input DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 6100)


lines.pprint()


# Start the computation
ssc.start()            
# Wait for the computation to terminate
ssc.awaitTermination()  

ssc.stop(stopSparkContext=False, stopGraceFully=True)
