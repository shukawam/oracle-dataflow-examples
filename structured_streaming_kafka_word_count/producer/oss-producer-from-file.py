import datetime
import re
import time
import os
from base64 import b64encode, b64decode

import oci

def get_stream(client, compartment_id, stream_name, stream_admin_client_composite):
    list_streams = client.list_streams(compartment_id=compartment_id, name=stream_name,
                                       lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("An active stream {} has been found".format(stream_name))
        stream_id = list_streams.data[0].id
        print(stream_id)
        return stream_admin_client_composite.client.get_stream(stream_id)
    else:
        print("Something is failed...")

def publish_massage(client, stream_id):
    print("Streaming... (Press Ctrl+C to cancel)")
    file_upload_iteration = 0
    while True:
        time_key = datetime.datetime.utcnow()
        print(time_key)
        file_upload_iteration += 1
        print(f"Iteration {file_upload_iteration}, key={time_key}",
            end=" ", flush=True)
        iteration_start_time = time.time()
        lines_counter = 0
        message_list = []
        with open(source_file_path) as fp:
            for line in fp:
                if lines_counter > read_lines_limit:
                    break

                word_list = list(filter(None, re.split("\W+", line.strip())))
                if not word_list:
                    continue

                text = time_key.isoformat() + " " + ' '.join(word_list)
                print(text)
                encoded_key = b64encode(time_key.strftime('%Y-%m-%d %H:%M:%S').encode()).decode()
                encoded_value = b64encode(text.encode()).decode()
                message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))
                message = oci.streaming.models.PutMessagesDetails(
                    messages = message_list
                )
                # message = oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value)
                put_message_result = client.put_messages(stream_id, message)
                lines_counter += 1
                # The put_message_result can contain some useful metadata for handling failures
                for entry in put_message_result.data.entries:
                    if entry.error:
                        print("Error ({}) : {}".format(entry.error, entry.error_message))
                    else:
                        print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))
        iteration_end_time = time.time()
        iteration_time_lapsed = iteration_end_time - iteration_start_time
        print(f" - Done. {iteration_time_lapsed:.2f} sec.")
        time_key += datetime.timedelta(seconds=cadense_sec)
        alignment_sleep_time = cadense_sec-iteration_time_lapsed
        if alignment_sleep_time > 0:
            time.sleep(alignment_sleep_time)
            
# Constants
# Download enwik8 from http://mattmahoney.net/dc/textdata.html or any other large enough text
source_file_path = '/tmp/enwik8'
read_lines_limit = 101209
cadense_sec = 60

stream_name = 'word-stream'

config = oci.config.from_file()
stream_admin_client = oci.streaming.StreamAdminClient(config)
stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

compartment_id = os.environ['C']

print("Streaming... (Press Ctrl+C to cancel)")

stream = get_stream(stream_admin_client, compartment_id, stream_name, stream_admin_client_composite).data

stream_client = oci.streaming.StreamClient(config, service_endpoint=stream.messages_endpoint)
stream_id = stream.id
publish_massage(stream_client, stream_id)