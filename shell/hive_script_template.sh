# !/usr/bin/bash

HADOOP_HOME=/your/hadoop/home
HIVE_HOME=/your/hive/home

SQL=$(cat<<!EOF

use db_test;
create table if not exists partition_table(
    id int,
    dname string
)
partitioned by (day string)
row format delimited fields terminated by '\t';

!EOF )
########### execute begin #############
echo $sql
cd $HIVE_HOME
bin/hive -e "$sql"

cd $HADOOP_HOME
bin/hdfs dfs -mkdir -p /hive/warehouse/db_test.db/partition_table/day=2019-03-01
bin/hdfs dfs -put /home/my/data/partition_table.txt /hive/warehouse/db_test.db/partition_table/day=2019-03-01

cd $HIVE_HOME
# 如果向分区表partition_table加入新的目录和内容，可以使用msck repair table命令将新分区加入表中
bin/hive -e "use db_test;msck repair table partition_table;"

exitCode=$?
if [ $exitCode -ne 0 ] ; then
    echo "[ERROR] hive execute failed!"
    exit $exitCode
fi