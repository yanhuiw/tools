hdfs_path="$1"

spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 20 --executor-memory 8g --executor-cores 4 \
hdfs_parse.py $hdfs_path