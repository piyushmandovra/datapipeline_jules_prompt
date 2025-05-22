import logging
import sys

def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Configures basic logging for the application.

    Args:
        log_level (int, optional): The minimum log level to capture.
                                   Defaults to logging.INFO.
        log_file (str, optional): Path to a file where logs should be written.
                                  If None, logs are only sent to the console.
                                  Defaults to None.
    """
    # Determine the format for the logs
    log_format = '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    
    # Basic configuration
    logging.basicConfig(level=log_level, format=log_format, stream=sys.stdout)

    if log_file:
        # If a log file is specified, add a FileHandler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)
        logging.info(f"Logging setup complete. Also logging to file: {log_file}")
    else:
        logging.info("Logging setup complete. Logging to console only.")

if __name__ == '__main__':
    # Demonstrate usage

    # Scenario 1: Default logging (INFO level, console only)
    print("--- Scenario 1: Default Logging (INFO to console) ---")
    # In a real app, setup_logging would be called once at the beginning.
    # For demonstration, we re-configure. To avoid multiple handlers in a real app if called multiple times,
    # one might first remove existing handlers from the root logger.
    # For this demo, basicConfig is sufficient if called once.
    # If setup_logging is called multiple times, basicConfig only has an effect the first time.
    # To make it re-configurable for this demo, let's clear handlers if any.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    setup_logging() # Default INFO level to console
    logger_console = logging.getLogger(__name__) # Get logger for the current module
    
    logger_console.debug("This is a debug message (Scenario 1 - Console, should not appear by default).")
    logger_console.info("This is an info message (Scenario 1 - Console).")
    logger_console.warning("This is a warning message (Scenario 1 - Console).")
    logger_console.error("This is an error message (Scenario 1 - Console).")
    logger_console.critical("This is a critical message (Scenario 1 - Console).")

    # Scenario 2: Logging to a file with DEBUG level
    print("\n--- Scenario 2: Logging to File (DEBUG level) ---")
    # Clear handlers again for a clean setup in this demo script
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_file_path = "test_app.log"
    setup_logging(log_level=logging.DEBUG, log_file=log_file_path)
    logger_file = logging.getLogger(__name__) # Get another logger instance

    logger_file.debug(f"This is a debug message (Scenario 2 - File: {log_file_path}).")
    logger_file.info(f"This is an info message (Scenario 2 - File: {log_file_path}).")
    logger_file.warning(f"This is a warning message (Scenario 2 - File: {log_file_path}).")

    print(f"Messages for Scenario 2 also written to '{log_file_path}'. Check its content.")

    # Scenario 3: Console only, DEBUG level
    print("\n--- Scenario 3: Console only (DEBUG level) ---")
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    setup_logging(log_level=logging.DEBUG)
    logger_debug_console = logging.getLogger(__name__)
    logger_debug_console.debug("This is a debug message (Scenario 3 - Console, DEBUG enabled).")
    logger_debug_console.info("This is an info message (Scenario 3 - Console).")

    print("\nLogging demonstration finished.")
