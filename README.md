# E-Commerce-End-To-End-Data-Engineering-Project
All Copyrights reserved to Omar Hossam Attia

# Kindly follow the steps to execute this E.L.T Project.

#STEP 1: GET KAFKA
Using this link https://www.apache.org/dyn/closer.cgi?path=/kafka/3.7.0/kafka_2.13-3.7.0.tgz download and install kafka via WSL 2 Distro.
$ cp from/your/download/path to/a/new/wsl/directory
$ tar -xzf kafka_2.13-3.7.0.tgz
$ cd kafka_2.13-3.7.0

#STEP 2: START THE KAFKA ENVIRONMENT
- Start the ZooKeeper service
$ bin/zookeeper-server-start.sh config/zookeeper.properties
- Start the Kafka broker service
$ bin/kafka-server-start.sh config/server.properties

#Step 3: Download and Install Apache Airflow
You can simply follow this YouTube video for installing Apache Airflow (https://www.youtube.com/watch?v=M2LNxxKPo3s&ab_channel=SiJo) Perfect Guide.

#Step 4: Start your Apache Airflow services
$ airflow scheduler
$ airflow webserver

#Step 5 DAG Directory: 
- Copy the file named "data-etl-pipeline.py" to your AIRFLOW Dags directory.

#Step 6 Run the DAG Python script:
$python3 location/to/your/data-elt-pipeline.py
