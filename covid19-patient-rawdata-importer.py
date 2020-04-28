import json, requests, os, logging
from telegram import Bot
from confluent_kafka import Producer

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


def acked(err, msg):
    if err is not None:
        logger.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))


def lambda_handler(event, context):
    covid19_api_raw_data_url = os.getenv('COVID19_API_RAW_DATA_URL')
    covid19_api_state_data_url = os.getenv('COVID19_API_STATE_DATA_URL')
    covid19_api_test_data_url = os.getenv('COVID19_API_TEST_DATA_URL')
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
    kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
    kafka_patient_data_topic_name = os.getenv('KAFKA_PATIENT_DATA_TOPIC_NAME')
    kafka_state_data_topic_name = os.getenv('KAFKA_STATE_DATA_TOPIC_NAME')
    kafka_test_data_topic_name = os.getenv('KAFKA_TEST_DATA_TOPIC_NAME')
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

    conf = {'bootstrap.servers': bootstrap_servers,
            'client.id': kafka_client_id,
            'linger.ms': '1000'}

    producer = Producer(conf, logger=logger)

    bot = Bot(token=telegram_bot_token)

    # import raw patient data from API
    resp = requests.get(url=covid19_api_raw_data_url)
    data = resp.json()

    # raw_data3.json starting point (27/04/2020)
    # patient number no longer represents a meaningful number. Just another id, treat it like that.
    patient_number = 27892
    for p in data['raw_data']:
        try:
            producer.produce(topic=kafka_patient_data_topic_name, value=json.dumps(p), key=str(patient_number),
                             on_delivery=acked)
            patient_number += 1
        except BufferError:
            logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))
        producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()
    raw_data_count = len(data['raw_data'])

    # import statewise testing data from API
    resp = requests.get(url=covid19_api_test_data_url)
    data = resp.json()
    test_data_count = 0
    old = None
    for p in data['states_tested_data']:
        try:
            if p['totaltested'] == '':
                continue
            key = dict({u'state': p['state'], u'date': p['updatedon']})
            if old is not None and p['state'] == old['state']:
                p['testreportedtoday'] = str(int(p['totaltested']) - int(old['totaltested']))
                if p['positive'] and old['positive']:  # this data can be missing
                    p['positivereportedtoday'] = str(int(p['positive']) - int(old['positive']))
                else:
                    p['positivereportedtoday'] = ""
            else:
                p['testreportedtoday'] = p['totaltested']
                if p['positive']:  # this data can be missing
                    p['positivereportedtoday'] = p['positive']
                else:
                    p['positivereportedtoday'] = ""
            test_data_count += 1
            old = p
            producer.produce(topic=kafka_test_data_topic_name, value=json.dumps(p), key=json.dumps(key),
                             on_delivery=acked)
        except BufferError:
            logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))
        producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()

    bot.send_message(chat_id=telegram_chat_id,
                     text='Imported {} patients and {} testing data into Kafka'.format(raw_data_count, test_data_count))


if __name__ == '__main__':
    event_req = dict()
    lambda_handler(event_req, None)
