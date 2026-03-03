import logging
import os


def setup_logging(name, log_dir="logs", log_filename="debug.log", *,
                  console_level=logging.INFO, file_level=logging.DEBUG,
                  silence_third_party=True, clear_file=True):
    """Configure and return a logger with file + console handlers.

    Parameters
    ----------
    name : str
        Logger name (typically ``__name__`` or a descriptive label).
    log_dir : str
        Directory for log files (created if missing).
    log_filename : str
        Name of the log file inside *log_dir*.
    console_level : int
        Minimum level for the console (StreamHandler).
    file_level : int
        Minimum level for the file handler.
    silence_third_party : bool
        If True, sets noisy third-party loggers to WARNING/ERROR.
    clear_file : bool
        If True, deletes the existing log file on startup.
    """
    os.makedirs(log_dir, exist_ok=True)

    # Remove any pre-existing root handlers to avoid duplicate output
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    log_path = os.path.join(log_dir, log_filename)
    if clear_file:
        try:
            if os.path.exists(log_path):
                os.remove(log_path)
        except Exception:
            pass

    file_handler = logging.FileHandler(log_path, mode='w')
    file_handler.setLevel(file_level)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    if silence_third_party:
        for lib in ('websockets', 'asyncio', 'urllib3', 'parso', 'fsspec'):
            logging.getLogger(lib).setLevel(logging.WARNING)
        logging.root.setLevel(logging.WARNING)

    return logger


def setup_summary_logger(log_dir="logs"):
    """Console + file logger for periodic summaries (used by gather_lighter_data)."""
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "debug.log")

    file_handler = logging.FileHandler(log_path, mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

    summary_logger = logging.getLogger('summary')
    summary_logger.handlers.clear()
    summary_logger.addHandler(console_handler)
    summary_logger.addHandler(file_handler)
    summary_logger.setLevel(logging.INFO)
    summary_logger.propagate = False

    return summary_logger


def setup_performance_logger(log_dir="logs"):
    """File-only logger for performance stats (used by gather_lighter_data)."""
    os.makedirs(log_dir, exist_ok=True)

    perf_handler = logging.FileHandler(
        os.path.join(log_dir, 'performance.log'), mode='w', encoding='utf-8'
    )
    perf_handler.setLevel(logging.INFO)
    perf_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

    perf_logger = logging.getLogger('performance')
    perf_logger.handlers.clear()
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)
    perf_logger.propagate = False

    return perf_logger
