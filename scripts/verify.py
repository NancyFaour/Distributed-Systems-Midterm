"""Exercise the local development stack with real messaging and HTTP requests."""
import os
from pathlib import Path
from uuid import uuid4

import pika
import requests
from kafka import KafkaConsumer, KafkaProducer


def main():
    settings = dict(
        line.split('=', 1)
        for line in (Path(__file__).resolve().parents[1] / '.env').read_text().splitlines()
        if line and not line.startswith('#')
    )
    settings.update(os.environ)
    marker = uuid4().hex.encode()

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost', credentials=pika.PlainCredentials(
            settings['RABBITMQ_USER'], settings['RABBITMQ_PASSWORD']),
        socket_timeout=10, blocked_connection_timeout=10))
    try:
        channel = connection.channel()
        queue = channel.queue_declare(queue='', exclusive=True).method.queue
        channel.basic_publish(exchange='', routing_key=queue, body=marker)
        method, _, body = channel.basic_get(queue, auto_ack=True)
        assert method and body == marker, 'RabbitMQ message did not match'
    finally:
        connection.close()
    print('PASS RabbitMQ publish/consume')

    topic = 'environment-smoke-check'
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', max_block_ms=30000,
                             request_timeout_ms=15000)
    try:
        sent = producer.send(topic, marker).get(timeout=30)
    finally:
        producer.close(timeout=10)
    consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9092',
                             enable_auto_commit=False, consumer_timeout_ms=15000)
    try:
        from kafka import TopicPartition
        partition = TopicPartition(topic, sent.partition)
        consumer.assign([partition])
        consumer.seek(partition, sent.offset)
        received = next(consumer, None)
        assert received and received.value == marker, 'Kafka message did not match'
    finally:
        consumer.close()
    print('PASS Kafka publish/consume')

    response = requests.get('http://localhost:9090/api/v1/targets', timeout=10)
    response.raise_for_status()
    targets = response.json()['data']['activeTargets']
    for job in ('prometheus', 'rabbitmq'):
        assert any(t['labels']['job'] == job and t['health'] == 'up' for t in targets), job
    print('PASS Prometheus scrapes itself and RabbitMQ')

    auth = (settings['GRAFANA_USER'], settings['GRAFANA_PASSWORD'])
    response = requests.get('http://localhost:3000/api/datasources/uid/prometheus/health',
                            auth=auth, timeout=15)
    response.raise_for_status()
    assert response.json()['status'] == 'OK', response.text
    print('PASS Grafana connects to Prometheus')


if __name__ == '__main__':
    main()
