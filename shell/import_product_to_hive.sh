# !/usr/bin/bash

# 使用sqoop将mysql数据库中的sqoop.product表导入hive分区表中，每天一个快照
###### 执行不成功

# HADOOP_HOME=/your/hadoop/home
# HIVE_HOME=/your/hive/home

# 分区表的位置放在: /user/4800613/sqoop/external/product
# 1. 建立hive多分区表 product
sql=$(cat<<!EOF

create external table if not exists product(
    price int,
    product_id string,
    product_name string
) 
partitioned by(
    day string   -- "每天一个分区，保存当天的快照"
)
 row format delimited fields terminated by '\t'
 stored as textfile
 location '/user/4800613/sqoop/external/product';

!EOF )
########### execute begin #############
echo $sql
cd $HIVE_HOME
bin/hive -e "$sql"

# 2. sqoop导出mysql数据到HDFS文件
sqoop import --connect "jdbc:mysql://10.173.32.6:3306/sqoop?characterEncoding=UTF-8" \
--username root --password Gg/ru,.#5 \
--table product \
--fields-terminated-by '\t' \
-m 1 \
--target-dir '/user/4800613/sqoop/external/product/day=2019-02-28' \
--delete-target-dir

# 3. 向分区中插入数据
day=$(date +%F)
sql=$(cat<<!EOF 

alter table task.product add partition if not exists (day="$day") 
location '/user/4800613/sqoop/external/product/day="$day"';

!EOF )

cd $HIVE_HOME
bin/hive -e "$sql"

exitCode=$?
if [ $exitCode -ne 0 ] ; then
    echo "[ERROR] hive execute failed!"
    exit $exitCode
fi