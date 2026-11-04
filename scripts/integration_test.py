#!/usr/bin/env python3
"""Real-broker checks. Requires a built solution, RabbitMQ Management, and Python 3."""
import base64
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
QUEUE = 'orders-test-' + uuid.uuid4().hex[:12]
USER = os.getenv('RabbitMq__UserName', os.getenv('RABBITMQ_USER', 'orderdev'))
PASSWORD = os.getenv('RabbitMq__Password', os.getenv('RABBITMQ_PASSWORD', 'orderdev_password'))
MANAGEMENT = os.getenv('RABBITMQ_MANAGEMENT_URL', 'http://localhost:15672').rstrip('/')
VHOST = urllib.parse.quote(os.getenv('RabbitMq__VirtualHost', '/'), safe='')
AUTH = base64.b64encode(f'{USER}:{PASSWORD}'.encode()).decode()


def http(url, method='GET', data=None, management=False):
    headers = {'Content-Type': 'application/json'}
    if management:
        headers['Authorization'] = 'Basic ' + AUTH
    request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                     headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read()
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def management(path, method='GET', data=None):
    status, body = http(MANAGEMENT + '/api/' + path, method, data, management=True)
    if status >= 400:
        raise RuntimeError(f'Management {method} {path}: HTTP {status}: {body}')
    return body


def wait_for(check, description, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(0.3)
    raise AssertionError('Timed out: ' + description)


def main():
    # Fail early with a useful error when Docker/RabbitMQ is unavailable.
    management('overview')
    for project in ('OrderProducer.Api', 'OrderConsumer.Worker'):
        if not (ROOT / project / 'bin/Debug/net10.0' / (project + '.dll')).exists():
            raise RuntimeError('Run dotnet build OrderMessagingSystem.slnx first (Debug configuration).')
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    api_url = f'http://127.0.0.1:{port}'
    env = os.environ | {
        'RabbitMq__QueueName': QUEUE, 'RabbitMq__UserName': USER,
        'RabbitMq__Password': PASSWORD, 'ASPNETCORE_URLS': api_url,
        'ASPNETCORE_ENVIRONMENT': 'Production', 'DOTNET_ENVIRONMENT': 'Production'
    }
    processes = []
    queue_path = f'queues/{VHOST}/{QUEUE}'
    with tempfile.TemporaryDirectory(prefix='order-messaging-tests-') as directory:
        log_paths = []
        log_files = []

        def start(project):
            path = Path(directory) / f'{len(log_paths)}-{project}.log'
            log_paths.append(path)
            output = path.open('w')
            log_files.append(output)
            process = subprocess.Popen(['dotnet', str(ROOT / project / 'bin/Debug/net10.0' / (project + '.dll'))],
                                       cwd=ROOT / project, env=env, stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process, path

        def stop(process):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError('Application did not stop gracefully')

        def queue():
            return management(queue_path)

        def post_order():
            status, body = http(api_url + '/orders', 'POST', {'customerName': 'Sopanha', 'totalAmount': 50})
            assert status == 202, (status, body)
            return body['orderId']

        try:
            start('OrderProducer.Api')
            worker, worker_log = start('OrderConsumer.Worker')
            wait_for(lambda: http(api_url + '/health')[0] == 200, 'API startup')
            wait_for(lambda: 'Waiting for orders' in worker_log.read_text(), 'consumer startup')
            first_id = post_order()
            print('PASS 1: API returned 202 after publishing an order')
            wait_for(lambda: f'Processed and acknowledged order {first_id}' in worker_log.read_text(), 'processing first order')
            print('PASS 2: Consumer received, processed and acknowledged the order')
            status, _ = http(api_url + '/orders', 'POST', {'customerName': '', 'totalAmount': -1})
            assert status == 400, status
            print('PASS 3: Invalid order returned 400')

            stop(worker)
            wait_for(lambda: queue()['consumers'] == 0, 'consumer shutdown')
            ids = [post_order() for _ in range(3)]
            print('PASS 4: Producer published three orders while consumer was stopped')
            wait_for(lambda: queue().get('messages_ready') == 3, 'three queued orders')
            assert queue()['durable'] is True
            sample = management(queue_path + '/get', 'POST', {
                'count': 1, 'ackmode': 'ack_requeue_true', 'encoding': 'auto', 'truncate': 50000
            })
            assert len(sample) == 1 and sample[0]['properties']['delivery_mode'] == 2
            assert json.loads(sample[0]['payload'])['orderId'] in ids
            print('PASS 5: Durable queue retained the orders; message persistence flag is set')

            worker, worker_log = start('OrderConsumer.Worker')
            wait_for(lambda: all(f'Processed and acknowledged order {oid}' in worker_log.read_text() for oid in ids),
                     'all queued orders processed after restart')
            wait_for(lambda: queue().get('messages') == 0, 'queue drained')
            print('PASS 6: Restarted consumer processed all queued orders')
            result = management(f'exchanges/{VHOST}/amq.default/publish', 'POST', {
                'properties': {'delivery_mode': 2, 'content_type': 'application/json'},
                'routing_key': QUEUE, 'payload': '{invalid-json', 'payload_encoding': 'string'
            })
            assert result['routed'] is True
            wait_for(lambda: management(f'queues/{VHOST}/{QUEUE}.dead-letter').get('messages_ready') == 1,
                     'invalid JSON routed to DLQ')
            dead = management(f'queues/{VHOST}/{QUEUE}.dead-letter/get', 'POST', {
                'count': 1, 'ackmode': 'ack_requeue_true', 'encoding': 'auto', 'truncate': 50000
            })
            assert dead[0]['payload'] == '{invalid-json'
            assert dead[0]['properties']['headers']['x-death'][0]['reason'] == 'rejected'
            print('PASS 7: Failed delivery reached the dead-letter queue without a retry loop')
            print('All seven real-broker scenarios passed.')
        except Exception:
            for path in log_paths:
                print(f'\n--- {path.name} ---\n{path.read_text()[-12000:]}')
            raise
        finally:
            for process in processes:
                stop(process)
            for output in log_files:
                output.close()
            # Only remove this run's uniquely named test queues/exchange; never touch orders.
            for path in (queue_path, f'queues/{VHOST}/{QUEUE}.dead-letter', f'exchanges/{VHOST}/{QUEUE}.dlx'):
                try:
                    http(MANAGEMENT + '/api/' + path, 'DELETE', management=True)
                except OSError:
                    pass


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit(f'FAIL: {error}')
