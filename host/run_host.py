from app.host import Client
import pika
import json

def main():
    global client
    client = Client(param)
    
    #config de rabbitmq
    
    rabbitmq_exchange = config['rabbit_mq']['exchange']
    crds = pika.PlainCredentials(config['rabbit_mq']['user'],
                                 config['rabbit_mq']['pswd'])
    parameters = pika.ConnectionParameters(
        host=config['rabbit_mq']['host'],
        credentials=crds,
        heartbeat=0,
        blocked_connection_timeout=None)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    result = channel.queue_declare(exclusive=True)
    rabbitmq_queue = result.method.queue

    channel.exchange_declare(
        exchange=rabbitmq_exchange, exchange_type='direct')
    channel.queue_bind(
        exchange=rabbitmq_exchange, queue=rabbitmq_queue, routing_key='sample_key')

    channel.basic_consume(callback, queue=rabbitmq_queue)
    channel.start_consuming()
    
def callback(ch, method, props, body):
    body = json.loads(body)
    try:
        command = body['command']
        if command == "test":
            ch.basic_ack(delivery_tag=method.delivery_tag)

            try:
                data = body['data']
                [status, results] = client.fct_test(data)
                message = {}
                message['status'] = status
                message['results'] = results
            except KeyError:
                message = {}
                message['status'] = "ERROR_NO_DATA"
                message['results'] = ""
            print('Publishing answer')
            ch.basic_publish(
                exchange='',
                routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id),
                body=json.dumps(message))
        else:
            print("unknow command")
    except KeyError:
        print("no command")
        
 if __name__ == "__main__":
      config_file = open('config.json')
      config = json.load(config_file)
      main()
