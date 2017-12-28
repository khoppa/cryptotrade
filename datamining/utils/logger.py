""" Creates custom loggers.
"""
import logging

def create_logger(folder, output):
    """ Creates logger at FOLDER, with OUTPUT as the output file.
    """
    logger = logging.getLogger(folder)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(output)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

