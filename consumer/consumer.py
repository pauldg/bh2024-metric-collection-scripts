"""This script is a consumer that listens to the queue and processes the messages and pushes them to the InfluxDB database."""

import os
import sys
import argparse
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


def process_message(body, message, stats_queue) -> None:
    """
    Process the incoming message and push the data to the results list.
    """
    try:
        stats_queue.put(body)
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")


def consume_target(amqp_url: str, stats_queue) -> None:
    """
    Define a target function for each process to consume messages and aggregate them.
    """
    connection = connect_to_queue(amqp_url)

    if connection:
        vhost = get_vhost_name(amqp_url)
        routing_key = f"{vhost}-condor"
        exchange = Exchange(f"{vhost}-condor-exchange", type="direct")
        queue = Queue(name=f"{vhost}-condor-stats", exchange=exchange, routing_key=routing_key)

        with Consumer(connection, queues=queue, callbacks=[lambda body, message: process_message(body, message, stats_queue)]):
            while True:
                try:
                    connection.drain_events(timeout=5)
                except Exception as e:
                    print(f"Error consuming messages: {e}")


def main(job_conf_file: str) -> None:
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

    # Aggregate the results from all processes
    results = []
    while not stats_queue.empty():
        results.append(stats_queue.get())

    for result in results:
        print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume messages from AMQP queues and aggregate them.")
    parser.add_argument("job_conf_file", type=str, help="Path to the job configuration file (YAML).")
    args = parser.parse_args()

    main(args.job_conf_file)
