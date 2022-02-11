import datetime
import re
import time
from kafka import KafkaProducer

# Constants
# Download enwik8 from http://mattmahoney.net/dc/textdata.html or any other large enough text
source_file_path = '/tmp/enwik8'
kafka_bootstrap_server = '<kafka-bootstrap-server>'
kafka_topic = 'word-count-stream'
kafka_username = '<username>'
kafka_streampool_id = '<stream-pool-id>'
kafka_token = '<auth-token>'
read_lines_limit = 101209
cadense_sec = 60

print("Creating Kafka producer...", end=" ")
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_server,
    security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
    sasl_plain_username=kafka_username,
    sasl_plain_password=kafka_token)
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
    with open(source_file_path) as fp:
        for line in fp:
            if lines_counter > read_lines_limit:
                break

            word_list = list(filter(None, re.split("\W+", line.strip())))
            if not word_list:
                continue

            text = time_key.isoformat() + " " + ' '.join(word_list)
            producer.send(kafka_topic, key=time_key.isoformat().encode(
                'utf-8'), value=text.encode('utf-8'))
            lines_counter += 1

    iteration_end_time = time.time()
    iteration_time_lapsed = iteration_end_time - iteration_start_time
    print(f" - Done. {iteration_time_lapsed:.2f} sec.")
    time_key += datetime.timedelta(seconds=cadense_sec)
    alignment_sleep_time = cadense_sec-iteration_time_lapsed
    if alignment_sleep_time > 0:
        time.sleep(alignment_sleep_time)
