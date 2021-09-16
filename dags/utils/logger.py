import logging
import configparser
import os


class Logger(object):
    def __init__(self, config=None, level='DEBUG', name=None):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Walsin_info.ini'),
                         encoding='utf-8-sig')
        self.logger = logging.getLogger(self.config['logger']['name']) if name is None else logging.getLogger(name)
        self.logger.setLevel(level)
        formatter = logging.Formatter('[%(name)s] %(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(self.config['logger']['filename'])
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

    def get_logger(self):
        return self.logger

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def critical(self, msg):
        self.logger.critical(msg)
