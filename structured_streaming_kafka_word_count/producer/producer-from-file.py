import datetime
import re
import time
from kafka import KafkaProducer
from argparse import ArgumentParser

def parser():
    usage = 'python {} [--input] [--bootstrap-servers] [--topic] [--username] [--password]'
    argparser = ArgumentParser(usage=usage)
    argparser.add_argument('--input', type=str, default='/tmp/enwik8', required=False)
    argparser.add_argument('--bootstrap-servers', type=str, required=True)
    argparser.add_argument('--topic', type=str, required=True)
    argparser.add_argument('--username', type=str, required=True)
    argparser.add_argument('--password', type=str, required=True)
    args = argparser.parse_args()
    return args

def publish_to_kafka(args):
    input = args.input
    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    username = args.username
    password = args.password
    read_lines_limit = 101209
    cadense_sec = 60
    print("Creating Kafka producer...", end=" ")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
        sasl_plain_username=username,
        sasl_plain_password=password)
    print("Done.")
    print("Streaming... (Press Ctrl+C to cancel)")
    file_upload_iteration = 0
    while True:
        time_key = datetime.datetime.utcnow()
        file_upload_iteration += 1
        print(f"Iteration {file_upload_iteration}, key={time_key}",
          end=" ", flush=True)
        iteration_start_time = time.time()
        lines_counter = 0
        with open(input) as fp:
            for line in fp:
                if lines_counter > read_lines_limit:
                    break
                word_list = list(filter(None, re.split("\W+", line.strip())))
                if not word_list:
                    continue
                text = time_key.isoformat() + " " + ' '.join(word_list)
                producer.send(topic, key=time_key.isoformat().encode(
                    'utf-8'), value=text.encode('utf-8'))
                lines_counter += 1
        iteration_end_time = time.time()
        iteration_time_lapsed = iteration_end_time - iteration_start_time
        print(f" - Done. {iteration_time_lapsed:.2f} sec.")
        time_key += datetime.timedelta(seconds=cadense_sec)
        alignment_sleep_time = cadense_sec-iteration_time_lapsed
        if alignment_sleep_time > 0:
            time.sleep(alignment_sleep_time)

if __name__ == '__main__':
    args = parser()
    publish_to_kafka(args)