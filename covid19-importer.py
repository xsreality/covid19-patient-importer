import json, requests, os
from telegram import Bot
from confluent_kafka import Producer


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Information about patient %s written to Kafka." % (str(msg.key())))


def lambda_handler(event, context):
    covid19_api_url = os.getenv('COVID19_API_URL')
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
    kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
    kafka_topic_name = os.getenv('KAFKA_TOPIC_NAME')
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

    bot = Bot(token=telegram_bot_token)

    resp = requests.get(url=covid19_api_url)
    data = resp.json()

    conf = {'bootstrap.servers': bootstrap_servers,
            'client.id': kafka_client_id}

    producer = Producer(conf)

    for p in data['raw_data']:
        producer.produce(topic=kafka_topic_name, value=json.dumps(p), key=p['patientnumber'], on_delivery=acked)
        producer.poll(1)
        break

    bot.send_message(chat_id=telegram_chat_id, text='Imported {} patients into Kafka'.format(len(data['raw_data'])))


if __name__ == '__main__':
    event_req = dict()
    lambda_handler(event_req, None)
