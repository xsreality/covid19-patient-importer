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
    covid19_api_state_data_url = os.getenv('COVID19_API_STATE_DATA_URL')
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
    kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
    kafka_state_data_topic_name = os.getenv('KAFKA_STATE_DATA_TOPIC_NAME')
    kafka_test_data_topic_name = os.getenv('KAFKA_TEST_DATA_TOPIC_NAME')
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

    conf = {'bootstrap.servers': bootstrap_servers,
            'client.id': kafka_client_id,
            'linger.ms': '1000'}

    producer = Producer(conf, logger=logger)
    bot = Bot(token=telegram_bot_token)

    # import statewise cases data from API
    state_data_resp = requests.get(url=covid19_api_state_data_url)
    state_data_json = state_data_resp.json()
    for s in state_data_json['statewise']:
        try:
            producer.produce(topic=kafka_state_data_topic_name, value=json.dumps(s), key=s['state'], on_delivery=acked)
        except BufferError:
            logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))
        producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()

    # import TOTAL testing data from API
    test_data_count = 0
    old = None
    for s in state_data_json['tested']:
        try:
            if s['totalsamplestested'] == '':
                continue
            key = dict({u'state': 'Total', u'date': s['updatetimestamp'].split(' ')[0]})
            value = dict({u'source': s['source'], u'state': 'Total', u'totaltested': s['totalsamplestested'],
                          u'positive': s['totalpositivecases'], u'testpositivityrate': s['testpositivityrate'],
                          u'updatedon': s['updatetimestamp'].split(' ')[0]})
            if old is not None:
                value['testreportedtoday'] = str(int(s['totalsamplestested']) - int(old['totalsamplestested']))
                if s['totalpositivecases'] and old['totalpositivecases']:  # this data can be missing
                    value['positivereportedtoday'] = str(
                        int(s['totalpositivecases'].replace(',', '')) - int(old['totalpositivecases'].replace(',', '')))
                else:
                    value['positivereportedtoday'] = ""
            else:
                value['testreportedtoday'] = s['totalsamplestested']
                if s['totalpositivecases']:  # this data can be missing
                    value['positivereportedtoday'] = s['totalpositivecases']
                else:
                    value['positivereportedtoday'] = ""
            old = s
            test_data_count += 1
            producer.produce(topic=kafka_test_data_topic_name, value=json.dumps(value), key=json.dumps(key),
                             on_delivery=acked)
        except BufferError:
            logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))
        producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()

    # send update on TG channel
    bot.send_message(chat_id=telegram_chat_id,
                     text='Imported {} states data and {} total testing data into Kafka'.format(
                         len(state_data_json['statewise']), test_data_count))


if __name__ == '__main__':
    event_req = dict()
    lambda_handler(event_req, None)
