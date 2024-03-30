import logging


def setup_logger(
        name='application_logger',
        level=logging.INFO,
        log_format='%(asctime)s - %(levelname)s - %(message)s'
    ):
    """
    Sets up and returns a logger.

    Args:
        name (str): Name of the logger.
        level (logging.LEVEL): Logging level.
        log_format (str): Format for logging messages.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Check if handlers are already added to avoid duplication
    if not logger.handlers:
        # Create a console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)

        # Create formatter and add it to the handler
        formatter = logging.Formatter(log_format)
        ch.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(ch)

    return logger
