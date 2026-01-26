from __future__ import annotations

import logging


def main_logger() -> logging.Logger:
    """
    Create custom logger with console and file handlers.
    
    Prevents duplicate handler addition by checking if handlers already exist.

    Returns:
        logging.Logger: Configured logger object for main application logging.
    """
    logger = logging.getLogger("datamart_framework.log")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        console_logger = logging.StreamHandler()
        console_logger.setLevel(logging.DEBUG)
        console_logger.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        file_handler = logging.FileHandler("datamart_framework.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        logger.addHandler(file_handler)
        logger.addHandler(console_logger)

    return logger


def debug_logger() -> logging.Logger:
    """
    Create debug logger for detailed diagnostic logging.
    
    Prevents duplicate handler addition by checking if handlers already exist.

    Returns:
        logging.Logger: Configured logger object for debug logging.
    """
    logger = logging.getLogger("datamart_framework.debug")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler("datamart_framework_debug.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        logger.addHandler(file_handler)

    return logger


logger = main_logger()
d_logger = debug_logger()
