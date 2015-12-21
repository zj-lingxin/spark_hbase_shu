source /etc/profile

mysql_host="192.168.7.7:3306"
mysql_username="root"
mysql_password="password"

PROJECT_HOME=/data/work/shu
PROJECT_LOG_HOME=$PROJECT_HOME/logs

date_cur=`date +%Y%m%d`
time_cur=`date +%s`
year_month=`date +%Y%m`
day=`date +%d`


if [ ! -d $PROJECT_LOG_HOME/offline/$date_cur ];then
   mkdir -p $PROJECT_LOG_HOME/offline/$date_cur
fi

echo "start!!"${store_id}" time:"${time_cur} &>> $PROJECT_LOG_HOME/$date_cur/process.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_host/asto_ec_origin?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password  $mysql_password --query 'select * from datag_sycm_shu where gmt_target < "2015-06-01 00:00:00" and $CONDITIONS'   -m 1  --target-dir /shu/input/offline/$year_month/$day/$time_cur/datag_sycm_shu --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_host/asto_ec_origin?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password  $mysql_password --query 'select * from datag_sycm_shu_new where $CONDITIONS'   -m 1  --target-dir /shu/input/offline/$year_month/$day/$time_cur/datag_sycm_shu_new --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_host/asto_ec_web?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password  $mysql_password --query 'select * from linkage where $CONDITIONS'   -m 1  --target-dir /shu/input/offline/$year_month/$day/$time_cur/linkage --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

hdfs dfs -ls /shu/input/offline/$year_month/$day/$time_cur

spark-submit  --master=local --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.lucius.shu.base.Main $PROJECT_HOME/dmp_shu.jar $time_cur saveMiddleFiles  >> $PROJECT_LOG_HOME/offline/$date_cur/spark_shu.log

hdfs dfs -ls /shu/input/offline/$year_month/$day/

echo "exec end"