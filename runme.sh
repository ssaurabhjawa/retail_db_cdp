export PYSPARK_PYTHON=python3
export ENVIRON=PROD
export user=${USER}
export SRC_DIR="/home/$user/retail_db_cdp/src/main/resources/retail-topic/"
export APP_NAME='retail_db_poc'
python3 src/main/python/kafka_app.py
