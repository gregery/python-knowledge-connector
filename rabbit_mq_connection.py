import json
import pika

class RabbitMQConnection:
    connection = None
    channel = None

    def __init__(self, config_filename):
        required_keys = ['host','port','queue_name','username','password']
        with open(config_filename, mode='rt', encoding='utf-8') as config_file:
            self.config_params = json.load(config_file)['rabbitmq_config']
            for required_key in required_keys:
                if required_key not in self.config_params:
                    raise Exception('config is missing required key: ' + required_key)

    def __del__(self):
        self.shutdown()

    def connect(self):
        credentials = pika.PlainCredentials(username=self.config_params['username'],
                                            password=self.config_params['password'])
        self.connection = pika.BlockingConnection(
                               pika.ConnectionParameters(host=self.config_params['host'],
                                                         port=self.config_params['port'],
                                                         credentials=credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.config_params['queue_name'])

    def run(self, callback):
        self.channel.basic_consume(queue=self.config_params['queue_name'], 
                                   auto_ack=False, 
                                   on_message_callback=callback)    
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('Received ctrl-C, exiting...')

    def shutdown(self):
        if self.connection is not None:
            self.connection.close()
        self.channel = None
        self.connection = None
