from typing import AnyStr

import os
import sys
import logging
from pathlib import Path

global outputs
global entry_point


def check_value(value: AnyStr):
    if value == 'local':
        pass
    elif value == 'prod':
        pass
    else:
        raise ValueError(f"value must be either (local|prod)")
    return value


def add_to_path(path: AnyStr):
    sys.path.insert(0, path)

    
def set_log_level(log_level: AnyStr):
    if log_level == 'info':
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    elif log_level == 'debug':
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    elif log_level == 'error':
        logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
    else:  # default
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

        
def set_output_directory(value):
    global outputs

    if value == 'local':
        outputs = (Path(os.getcwd()).parents[1] / "testing" / "outputs").as_posix()
    elif value == 'prod':
        outputs = (Path(os.getcwd()).parents[1] / "testing" / "outputs").as_posix()

        
def set_lmax_reporting_entry_point(value: AnyStr):
    global entry_point

    if value == 'local':
        entry_point = (Path(os.getcwd()).parents[1] / "backtesting").as_posix()
    elif value == 'prod':
        entry_point = (Path(os.getcwd()).parents[1] / "backtesting").as_posix()


def setup(value: AnyStr, log_level: AnyStr = 'info'):
    # set up environment
    value = check_value(value)
    
    # add to path
    if value == 'local':
        add_to_path((Path(os.getcwd()).parents[2] / 'backtesting').as_posix())
    else:
        conda_path = input("Enter Location of Conda Environment:")
        add_to_path(conda_path)
    
    # set lmax_reporting_flows_entry_point
    set_output_directory(value)
    set_lmax_reporting_entry_point(value)

    # set logging level
    set_log_level(log_level)



















