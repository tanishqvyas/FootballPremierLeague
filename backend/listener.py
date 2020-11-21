from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", appName="FantasyLeagueAnalysis")
ssc = StreamingContext(sc, 1)

# Create an input DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 6100)

words = lines.map(lambda line: line)

words.pprint()




# Start the computation
ssc.start()            
# Wait for the computation to terminate
ssc.awaitTermination()  


print("hello")