from functools import update_wrapper
import logging 
from typing import Any, Optional 
from logging import getLogger
from time import perf_counter 
from datetime import datetime
from zlib import DEF_BUF_SIZE 
import sys

from sympy import arg

class logger:
    """
    Quick decorator to manage logging returns of various functions.
    """

    def __init__(self,fileName:str = r"C:\Users\hq001381\bitbucket\master\cchbc\utils\log_file.log",  alarm_lvl:str = "debug")->None:
        logging.basicConfig(level=logging.DEBUG)
        # Get or Create a logger
        self.logger = getLogger()
        self.alarm_lvl = alarm_lvl

        # define file and format of logger
        file_handler = logging.FileHandler(fileName)
        formatter = logging.Formatter('%(asctime)s::%(lineno)d::%(message)s')
        file_handler.setFormatter(formatter)

        # Add file handler to logger
        self.logger.addHandler(file_handler)
        
    def __call__(self, func:callable):
        update_wrapper(self, func)
        self.func = func 
        return getattr(self, self.alarm_lvl)

        

    def debug(self, *args, **kwargs):
        
        self.logger.debug(f"Running:{self.func.__name__}(type:{type(self.func)}) with id {id(self)} at {datetime.now()}")
        start = perf_counter()

        value = self.func(*args, **kwargs)
    
        end = perf_counter()
        self.logger.debug(f"""Completed {self.func.__name__} with id {id(self)} at {datetime.now()},  Total Time to run {end - start:.6f}s""")

        return self

    def info(self, *args, **kwargs):
        self.logger.info(f"Running {self.func.__name__} at {datetime.now()}") 
        return self.func(*args, **kwargs)

    def warning(self, *args, **kwargs):pass 

    def error(self, *args, **kwargs):pass 

    def critical(self, *args, **kwargs):pass 

