# tpch-spark (AVRO FILESYSTEM FORMAT)

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.0.0

# SetUp

add following library to `jars/` folder:

```bash
wget http://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar
mv snappy-java-1.1.4.jar /usr/spark-2.3.0/jars/
```

install sbt:

```bash
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt update
sudo apt install sbt
```

install dstat:

```bash
apt update; apt install -y dstat
```

### Running

First compile using:

```bash
sbt package
```

You can then run a query using:

```bash
spark-submit --class "main.scala.TpchQuery" --master spark://master:7077 target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar [num1] [num2]
```

where `[num1]` and `[num2]` is the number of the query to run e.g 0, 1, 2, ..., 22 (query from `[num1]` to `[num2]` will be executed)
and MASTER specifies the spark-mode e.g local, yarn, standalone etc...


### Other Implementations

1. Data generator (http://www.tpc.org/tpch/)

2. TPC-H for Hive (https://issues.apache.org/jira/browse/hive-600)

3. TPC-H for PIG (https://github.com/ssavvides/tpch-pig)
