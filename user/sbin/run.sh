#########################################################################
# File Name: run.sh
# Author: fjp
# mail: fjp@dangdang.com
# Created Time: Mon 18 Nov 2019 07:08:30 PM CST
#########################################################################
#!/bin/bash

CUR_DIR=$(cd `dirname $0`; pwd)
#last_dt=`date -d "1 days ago" +%Y-%m-%d`
#last_dt=`date  +"%Y-%m-%d %H:%M.%S"`
last_dt=`date  +"%Y-%m-%d"`

outPath1="hdfs://10.5.25.148:8020/data/userData/old_new_$last_dt"
outPath2="hdfs://10.5.25.148:8020/data/userData/new_old_$last_dt"

oldPath="hdfs://10.7.28.227:9000/groups/adsystem/zhangbingquan/cpc_new/2019-11-18"
newPath="hdfs://10.5.25.148:8020/data/userData/cpc/2019-11-18"

spark-submit --name test_fjp \
	--driver-java-options "-Dlog4j.configuration=file://$CUR_DIR/log4j.properties" \
	--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$CUR_DIR/log4j.properties" \
	--class com.dangdang.hdfsDiff \
	--master yarn \
	--deploy-mode client \
	--num-executors 50 \
	--executor-memory 4G \
	--executor-cores 2 \
	--driver-memory 10G \
	--conf spark.default.parallelism=100 \
	--conf spark.ui.port=18080 \
	./user-1.0-SNAPSHOT-jar-with-dependencies.jar \
	$outPath1 $outPath2 $oldPath $newPath

