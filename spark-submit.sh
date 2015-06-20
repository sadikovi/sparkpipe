#--master "local-cluster[4, 1, 512]" \
spark-submit \
    --master "local[*]" \
    --driver-memory 1g \
    --executor-memory 1g \
    --class "org.spark.closure.Main" \
    target/scala-2.10/sparkpipe-app_2.10-0.0.1.jar
