import os
from confluent_kafka import Producer
import time


def send_data_to_topic(src_dir):
    conf = {'bootstrap.servers': 'cdp03.itversity.com:9092,cdp04.itversity.com:9092,cdp05.itversity.com:9092'}
    p = Producer(conf)
    for entry in os.listdir(src_dir):
        file_dir = os.path.join(src_dir, entry)
        for filesInDir in os.listdir(file_dir):
            table_folder = os.path.join(file_dir, filesInDir)
            retail_db_files = open(table_folder)
            for line in retail_db_files:
                p.produce('retail_db', key=entry, value=line)
                print(f'{line}')
                time.sleep(1)

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            p.flush()



