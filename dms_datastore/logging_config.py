import logging
import os

def setup_logger(console_log_level=logging.INFO, file_log_level=logging.DEBUG, 
                  log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"):
    
    file_log_name= "logger_datastore.log"
    
    # Create a logger instance
    logger = logging.getLogger(__name__)

    # Set the logging level (choose appropriate level based on the needs)
    logger.setLevel(logging.DEBUG)

    # Create a file handler to log messages to a file
    file_handler = logging.FileHandler(file_log_name)
    file_handler.setLevel(file_log_level)

    # Create a stream handler to print messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)


    # Create a formatter for the log messages
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    console_formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
    # Attach the formatter to both handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(console_formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    #
    return logger

# setup at module level as there is no classes defined
logger = setup_logger(log_format="%(asctime)s - %(message)s")
