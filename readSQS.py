import boto3
import psycopg2
import json
from datetime import datetime

def mask_pii(value):
    if len(value) < 4:
        return "*" * len(value)
    return value[:2] + "*" * (len(value) - 4) + value[-2:]


def read_from_queue():
    sqs = boto3.client('sqs', endpoint_url='http://localhost:4566', region_name='us-west-2')
    queue_url = "http://localhost:4566/000000000000/login-queue"
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
    messages = response.get('Messages', [])
    print('Number of messages read from the queue:', len(messages))
    print("Messages:", messages)
    return messages


def write_to_postgres(messages):
    connection = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port='5432')
    cursor = connection.cursor()

    for message in messages:
        body = json.loads(message['Body'])
        user_id = body['user_id']
        device_type = body['device_type']
        masked_ip = mask_pii(body['ip'])
        masked_device_id = mask_pii(body['device_id'])
        locale = body['locale']
        app_version = body['app_version']
        create_date = datetime.strptime(body['create_date'], '%Y-%m-%d').date()
        
        cursor.execute("""
            INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date))
        
    connection.commit()
    cursor.close()
    connection.close()

def main():
    messages = read_from_queue()
    write_to_postgres(messages)

if __name__ == "__main__":
    main()



