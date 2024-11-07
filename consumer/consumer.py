"""This script is a consumer that listens to the queue and processes the messages and pushes them to the InfluxDB database."""

import os
import sys
import argparse
import time
from multiprocessing import Process, Manager

import yaml
from kombu import Connection, Consumer, Exchange, Queue


def get_pulsar_runners(job_conf_file: str) -> dict:
    """
    Parse the job_conf.yml file and extract the Pulsar runners and their AMQP URLs.
    """
    # Check the existence of the job_conf.yml file
    if not os.path.exists(job_conf_file):
        print(f"File {job_conf_file} does not exist.")
        return None

    with open(job_conf_file, "r") as file:
        job_conf = yaml.safe_load(file)

    pulsar_runners = {}
    for runner, values in job_conf["runners"].items():
        if runner.startswith("pulsar"):
            if "amqp_url" in values:
                pulsar_runners[runner] = values["amqp_url"]

    return pulsar_runners


def get_vhost_name(amqp_url: str) -> str:
    """
    Parse the AMQP URL to extract the vhost name.
    """
    vhost = amqp_url.split("/")[-1].split("?")[0]
    return vhost


def connect_to_queue(amqp_url: str) -> Connection:
    """
    Connect to the AMQP queue using the provided URL.
    """
    # With try and except block, connect to the AMQP queue using the provided URL and manage the error if the connection fails
    try:
        connection = Connection(amqp_url)
        connection.ensure_connection(max_retries=3)
        return connection
    except Exception as e:
        print(f"Error connecting to the AMQP queue: {e}")
        return None


def process_message(body, message, vhost, stats_queue) -> None:
    """
    Process the incoming message and push the data to the results list.
    """
    try:
        stats_queue.put({vhost: body})
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")



def consume_target(amqp_url: str, stats_queue) -> None:
    """
    Define a target function for each process to consume messages and aggregate them.
    """
    connection = connect_to_queue(amqp_url)

    if connection.connected:
        vhost = get_vhost_name(amqp_url)
        routing_key = f"{vhost}-condor"
        exchange = Exchange(f"{vhost}-condor-exchange", type="direct")
        queue = Queue(name=f"{vhost}-condor-stats", exchange=exchange, routing_key=routing_key)

        with Consumer(connection, queues=queue, callbacks=[lambda body, message: process_message(body, message, vhost, stats_queue)], accept=["json"]):
            while True:
                try:
                    connection.drain_events(timeout=2)
                except Exception as e:
                    print(f"Error consuming messages: {e}")
                    stats_queue.put({vhost: None})
    else:
        print(f"Failed to connect to the AMQP queue of {vhost}.")


def get_influxdb_conf_from_env() -> tuple:
    """
    Retrieve the InfluxDB configuration from the environment variables.
    """
    host = os.getenv("INFLUXDB_HOST")
    port = os.getenv("INFLUXDB_PORT")
    username = os.getenv("INFLUXDB_USERNAME")
    password = os.getenv("INFLUXDB_PASSWORD")
    database = os.getenv("INFLUXDB_DATABASE")
    measurement = os.getenv("INFLUXDB_MEASUREMENT")

    return host, port, username, password, database, measurement


def influxdb_connect(host: str, port: int, username: str, password: str, database: str, ssl: bool = True, verify_ssl: bool = True) -> InfluxDBClient:
    """
    Connect to the InfluxDB database using the provided parameters.
    """
    try:
        client = InfluxDBClient(host=host, port=port, username=username, password=password, ssl=ssl, verify_ssl=verify_ssl)
        databases = client.get_list_database()
        if database not in [db["name"] for db in databases]:
            print(f"Database {database} not found.")
            return None
        return client
    except Exception as e:
        print(f"Error connecting to the InfluxDB database: {e}")
        return None


def influxdb_get_last_entry(client: InfluxDBClient, measurement: str, destination_id: str) -> dict:
    """
    Fetch the last entry from the InfluxDB database for the given measurement and destination ID.
    """
    result = client.query(f'SELECT * FROM "{measurement}" WHERE "destination_id"=\'{destination_id}\' ORDER BY time DESC LIMIT 1')
    return list(result.get_points())[0]


def influxdb_get_measurement_metadata(client: InfluxDBClient, measurement: str) -> tuple:
    """
    Get the tag and field keys for the specified measurement from the InfluxDB database.
    """
    # Get tag keys
    tag_keys_result = client.query(f'SHOW TAG KEYS FROM "{measurement}"')
    tag_keys = [point['tagKey'] for point in tag_keys_result.get_points()]

    # Get field keys
    field_keys_result = client.query(f'SHOW FIELD KEYS FROM "{measurement}"')
    field_keys = [point['fieldKey'] for point in field_keys_result.get_points()]

    return tag_keys, field_keys


# def influxdb_insert_newentry(client: InfluxDBClient, measurement: str, destination_id: str, threshold: int = 600) -> None:
#     """
#     Check the last entry in the InfluxDB database and insert a new entry if the time difference is more than the threshold.
#     """
#     # Get tag and field keys for the measurement
#     tag_keys, field_keys = influxdb_get_measurement_metadata(client, measurement)

#     # Query the last entry for the specified destination_id
#     result = influxdb_get_last_entry(client, measurement, destination_id)
#     last_entry = list(result.get_points())[0]

#     # Get current epoch time
#     current_epoch = time.time()

#     # Check if the last entry's querytime is older than the specified threshold
#     if current_epoch - last_entry['querytime'] > threshold:
#         new_entry = {
#             "measurement": measurement,
#             "tags": {},
#             "time": time.time_ns(),
#             "fields": {}
#         }

#         # Populate tags and fields based on last_entry and metadata
#         for key, value in last_entry.items():
#             if key in tag_keys:
#                 if value is not None:
#                     new_entry['tags'][key] = value
#             elif key in field_keys:
#                 if key == 'querytime':
#                     new_entry['fields'][key] = current_epoch
#                 elif key == 'destination_id':
#                     new_entry['fields'][key] = destination_id
#                 elif key == 'destination_status':
#                     new_entry['fields'][key] = "offline"
#                 else:
#                     new_entry['fields'][key] = None

#         client.write_points([new_entry])
#         print(new_entry)


def build_influxline_protocol_entry(measurement: str, tags: dict, fields: dict, timestamp: int) -> str:
    """
    Format the data as an InfluxDB line protocol string.
    """
    tag_str = ",".join([f"{key}={value}" for key, value in tags.items()])
    field_str = ",".join([f"{key}={value}" if value is not None else f"{key}=" for key, value in fields.items()])

    return f"{measurement},{tag_str} {field_str} {timestamp}"


def influxdb_create_newentry(client: InfluxDBClient, measurement: str, destination_id: str, threshold: int = 600) -> str:
    """
    Check the last entry in the InfluxDB database and, if the time difference is more than the threshold,
    return a new entry formatted as an InfluxDB line protocol string.
    """
    # Retrieve metadata for tag and field keys
    tag_keys, field_keys = influxdb_get_measurement_metadata(client, measurement)

    # Get the last entry for the destination
    last_entry = influxdb_get_last_entry(client, measurement, destination_id)

    # Get current epoch time
    current_epoch = time.time()

    # Check if last entry is older than the threshold
    if current_epoch - last_entry['querytime'] > threshold:
        tags, fields = {}, {}

        for key, value in last_entry.items():
            if key in tag_keys:
                if value is not None:
                    tags[key] = value
            elif key in field_keys:
                if key == 'querytime':
                    fields[key] = current_epoch
                elif key == 'destination_id':
                    fields[key] = destination_id
                elif key == 'destination_status':
                    fields[key] = "offline"
                else:
                    fields[key] = None

        # Build and return influx line protocol entry
        line_protocol_entry = build_influxline_protocol_entry(measurement, tags, fields, time.time_ns())
        return line_protocol_entry
    else:
        return None


def main(job_conf_file: str, threshold: int) -> None:
    """
    Consume messages from multiple AMQP queues in parallel and aggregate the results.
    """
    amqp_urls = get_pulsar_runners(job_conf_file)

    if not amqp_urls:
        print("No Pulsar runners found in the job configuration file.")
        sys.exit(1)

    stats_queue = Manager().Queue()
    processes = []

    # Create a process for each AMQP URL
    for runner, amqp_url in amqp_urls.items():
        proc = Process(target=consume_target, args=(runner, amqp_url, stats_queue))
        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    # Get the InfluxDB configuration from the environment variables
    influxdb_host, influxdb_port, influxdb_username, influxdb_password, influxdb_database, influxdb_measurement = get_influxdb_conf_from_env()

    # Connect to the InfluxDB database
    client = influxdb_connect(influxdb_host, influxdb_port, influxdb_username, influxdb_password, influxdb_database)

    # Aggregate the results from all processes
    results = []
    while not stats_queue.empty():
        body = stats_queue.get()
        for vhost, data in body.items():
            if data:
                data['condor_metrics'] = f"{data['condor_metrics']},destination_status=online"
                results.append(data['condor_metrics'])
            else:
                new_entry = influxdb_create_newentry(client, influxdb_measurement, vhost, threshold)
                if new_entry:
                    results.append(new_entry)

    for result in results:
        print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume messages from AMQP queues and aggregate them.")
    parser.add_argument("job_conf_file", type=str, help="Path to the job configuration file (YAML).")
    parser.add_argument("--threshold", type=int, default=600, help="Time threshold in seconds for setting destination_status=offline.")
    args = parser.parse_args()

    main(args.job_conf_file, args.threshold)
