export PYSPARK_PYTHON=python3
export ENVIRON=PROD
export SRC_DIR='f"/home/$(whoami)/retail-topic/"'
export APP_NAME='retail_db_poc'
spark-submit \
--master yarn \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
--deploy-mode client \
--conf spark.ui.port=0 \
python/kafka_app.py

