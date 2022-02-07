from confluent_kafka import Consumer


def read_kafkaTopic(env, appName):
    conf = {'bootstrap.servers': 'w01.itversity.com:9092,w02.itversity.com:9092',
            'group.id': "foo",
            'auto.offset.reset': 'smallest'
            }
    c = Consumer(conf)

    c.subscribe(['retail_db'])
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
    c.close()
