# Football Premier League

Real time analysis of match event data using PySpark.


## Folder Structure

```
+
|---- backend
|	|
|	|---- main.py
|	|---- User_Interface.py
|	|
|   |---- request_response_data
|	|	|
|	|	|---- request.txt
|	|	|---- response.txt
|   |   
|   |
|	|---- utils
|	|	|
|	|	|---- helper.py
|
|---- data
|	|
|	|---- players.csv
|	|
|	|---- teams.csv
|	|
|	|---- stream
|	|	|
|	|	|---- eve.txt
|	|	|
|	|	|---- mat.txt
|	|	|
|	|	|---- stream.pyc 
|
|---- project information
|
|---- README.md
|
|---- .gitignore
|
|---- requirements.txt
|
+
```


## Setup

**1. Install Spark**

Install spark on your system by following the instructions given in **spark.md**


**2. Fork the Repsitory**

**3. Clone the forked repository**

**4. Setup the upstream**

```
git remote add upstream "https://github.com/tanishqvyas/FootballPremierLeague.git"
```

**5. Download eve.txt file from the Forums and place it in the directory as shown in folder structure**

**6. Open 2 terminals and run the stream_data.py and main.py files**