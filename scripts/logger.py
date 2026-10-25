import logging

def setup_logger(name, level=logging.DEBUG):
    """Function to setup a logger with a specific name and log file."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file = "logs/{}.log".format(name)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    

    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger