import os
import logging
import time
import uuid
import json
import pika
import logging.handlers
from multiprocessing.dummy import Pool


class Server:

    def __init__(self, cfg):

        self.host = cfg['rabbitMq']['host']
        self.port = cfg['rabbitMq']['port']
        self.exchange = cfg['rabbitMq']['exchange']
        self.usr = cfg['rabbitMq']['user']
        self.pwd = cfg['rabbitMq']['password']
        self.max_workers = cfg['server']['max_workers']

        filename = __name__ + '.log'

        # Logger initialisation
        self.logger = logging.getLogger(__name__)
        formatter = logging.Formatter(
            '[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
            '%d/%m/%Y %I:%M:%S %p')
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        fh = logging.handlers.TimedRotatingFileHandler(
            filename, when='w6', backupCount=24, delay=True)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        
        crds = pika.PlainCredentials(self.usr, self.pwd)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=crds,
                heartbeat=0,
                blocked_connection_timeout=None))
        self.logger.info('Connection with rabbitmq server etablished')

        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange=self.exchange, exchange_type='direct')

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            self.on_response, no_ack=True, queue=self.callback_queue)
        self.logger.info('Queue created')
        
    def on_response(self, ch, method, props, body):
        """Function that receive the answer from RabbitMq requests"""

        self.logger.info(
            'Consuming -- Correlation Id : %s' % props.correlation_id)
        self.logger.debug('Consuming --  Message : %s' % body)

        if props.correlation_id in self.corr_id:
            try:
                self.response.append((self.corr_workers[props.correlation_id],
                                      body))
            except:
                self.response.append(body)
                
    def fct_test(self):
        message = {}
        message['command'] = "test"
        message['data'] = "message"
        self.response = []
        self.corr_id = []
        corr = str(uuid.uuid4())
        self.corr_id.append(corr)
        persistent_data = shelve.open(self.LOCAL_DB, writeback=True)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key='sample_key',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr,
            ),
            body=json.dumps(message))
        self.logger.info('[reco] Publishing -- Correlation Id : %s' % corr)
        
        scores = []
        worker_res = None
        while len(self.response) < self.max_workers:
            self.connection.process_data_events()
            time.sleep(0.01)
       
        for res in self.response:
            worker_res = json.loads(res)
            status = worker_res['status']
        self.response = []
        self.logger.info(status)
        return self.result(status, [])
    
    def result(self, status, contents):
        """Function that store the results in a dictionnary"""
        result = {}
        result['status'] = status
        result['results'] = contents
        return result

