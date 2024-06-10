docker cp data/danh_sach_sv_de.csv namenode:hadoop-data

docker exec -it namenode bash

hdfs dfs -mkdir -p user/root/data

hdfs dfs -mkdir user/root/raw_zone/activity

hdfs dfs -mkdir user/root/result

hdfs dfs -put hadoop-data/danh_sach_sv_de.csv user/root/data
