"""
File name: weather_data.py
Author: Mahdi Bazyar
Date: 06/02/2024

"""

import os
import sys
import pandas as pd, time
import datetime
import json
import logging
import time

class base_functions(object):
    def __init__(self, caller: str, config: str) -> None:
        """
        Initilalize the object. read in configuration and create a logger object.
        param caller: string. name of the child class inheriting from this class.
        param config: string. file path for configuraton file in json format.
        return: None
        
        """
        try:
            # Date formats
            self.date_time_format = '%m/%d/%Y %H:%M:%S'
            self.postgres_date_time_format = 'MM/DD/YYYY HH24:MM:SS'

            # Base Date and Time
            self.date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d')
            self.time = datetime.datetime.fromtimestamp(time.time()).strftime('%H_%M_%S')

            # Load configuration file
            self.config_directory = os.path.dirname(config)
            self.config = self.read_config(config)

            # Establish logger based on 'caller'
            self.caller = caller
            # Logs will be stored in the same directory as configuration file
            self.log_directory = os.path.join(self.config_directory, 'logs', self.date)
            self.logger = self.get_logger()

        except Exception as e:
            print("Process failed at base_functions() class initialization. {0}".format(str(e)))

    def read_config(self, config: str) -> dict:
        """
        Read in configuation.
        param config: string. file path for configuraton file in json format
        return: Dictionary. the configuraton file as dictionary 
        
        """
        try:
            return config if isinstance(config, dict) else json.load(open(config))
        except ValueError as value_error:
            self.logger.error ("Configuration input {0} is not valid: {1}".format(config, str(value_error)))
            
    
    def get_logger(self) -> logging.Logger:
        """
        Get a logger to log DEBUG in command window and INFO in log file.
        Ensures logger_directory exists, build and return simple logger
        return: Logger object
        
        """
        try:
            start_time = time.time()
            the_logger = logging.getLogger(self.caller)
            the_logger.setLevel(logging.DEBUG)

            # Ensure directories exist 
            if not os.path.exists(self.log_directory):
                os.makedirs(self.log_directory)

            # Set Console Handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)

            # Set file handler
            caller_time = "{0}_{1}.log".format(self.caller, self.time)
            file_handler = logging.FileHandler(os.path.join(self.log_directory, caller_time), 'w')
            file_handler.setLevel(logging.INFO)

            # Format the log
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            
            # console_handler
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            the_logger.addHandler(console_handler)
            the_logger.addHandler(file_handler)

            the_logger.info("Logger initialized successfully.")
        
        except Exception as e:
            print("Process failed at get_logger() function. {0}".format(str(e)))

        return the_logger