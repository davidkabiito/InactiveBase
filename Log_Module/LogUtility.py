#Auther: Rudra Prasad
#For logging purpose

import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

class Logger(object):
    def __init__ (self, loggername, logfile):
        self.logger = logging.getLogger(loggername)
        self.logger.setLevel(logging.DEBUG)
        fileLogHandler = TimedRotatingFileHandler(logfile, 'midnight', 1, 31)
        fileLogHandler.setLevel(logging.DEBUG)
        logOutputFormat = logging.Formatter("%(asctime)-10s %(name)-10s %(levelname)-5s %(message)-30s")
        fileLogHandler.setFormatter(logOutputFormat)
        self.logger.addHandler(fileLogHandler)

    def setLog (self, level, message):
        if level == 'DEBUG':
            self.logger.debug(message)
        elif level == 'INFO':
            self.logger.info(message)
        elif level == 'WARNING':
            self.logger.warning(message)
        elif level == 'ERROR':
            self.logger.error(message)
        elif level == 'CRITICAL':
            self.logger.critical(message)

#END
