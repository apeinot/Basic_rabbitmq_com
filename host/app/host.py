import logging
import logging.handlers
import time

class Client:

    def __init__(self, param):

        #creation des loggers
        self.logger = logging.getLogger('worker_' + str(param['worker_id']))
        formatter = logging.Formatter(
            '[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
            '%d/%m/%Y %I:%M:%S %p')
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        fh = logging.handlers.TimedRotatingFileHandler(
            filename='client.log', when='w6', backupCount=24, delay=True)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        self.logger.addHandler(ch)
        self.logger.addHandler(fh)

        self.logger.info("CLIENT")
        
    def fct_test(self, data):
        time.sleep(2)
        self.logger(str(data))
        return ['Test_done', 'ok']
