import os
from read import send_data_to_topic
from consumer import read_kafkaTopic

def main():
    appName = os.environ.get('APP_NAME')
    env = os.environ.get('ENVIRON')
    src_dir=os.environ.get('SRC_DIR')
    send_data_to_topic(src_dir)
    read_kafkaTopic(env, appName)


if __name__ == '__main__':
    main()
