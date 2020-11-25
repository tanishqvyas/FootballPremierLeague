# Spark Installation (Ubuntu)

**1. Open a terminal window and run the following command to install all three packages at once:**
```
sudo apt install default-jdk scala git -y
```

**2. Once the process completes, verify the installed dependencies by running these commands:**

```
java -version; javac -version; scala -version; git --version
```

**3. Use the wget command and the direct link to download the Spark archive:**

```
sudo wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
```
When the download completes, you will see the saved message.


**4. Now, extract the saved archive using the tar command:**

```
tar xvf spark-*
```

**5. Finally, move the unpacked directory spark-3.0.1-bin-hadoop2.7 to the opt/spark directory.**
Use the mv command to do so:

```
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
```
The terminal returns no response if it successfully moves the directory.


## Configure Spark Environment

**Before starting a master server, you need to configure environment variables. There are a few Spark home paths you need to add to the user profile.**


**1. Open .profile using nano**

```
sudo nano .profile
```

**2. Scroll to the end of the file and paste the following 3 lines**

```
export SPARK_HOME=/opt/spark

export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

export PYSPARK_PYTHON=/usr/bin/python3
```

**3. Now that you have completed configuring your environment for Spark, you can start a master server. Type:**

```
start-master.sh
```

**4.To view the Spark Web user interface, open a web browser and enter the localhost IP address on port 8080.**

```
http://127.0.0.1:8080/
```

**5. Test Spark Shell by typing the following on the terminal**

```
spark-shell
```

**6.If you do not want to use the default Scala interface, you can switch to Python.**
Make sure you quit Scala and then run this command:

```
pyspark
```


## Reference

```
https://phoenixnap.com/kb/install-spark-on-ubuntu
```


