export PYSPARK_PYTHON=python3
export ENVIRON=PROD
export user=${USER}
export SRC_DIR="/home/$user/retail-topic/"
export APP_NAME='retail_db_poc'
unzip -d src/main/resources/retail-topic.zip
src/main/python/kafka_app.py
