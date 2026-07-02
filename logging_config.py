import atexit
import logging
import logging.handlers
import os
import queue

# Keep a reference per logger name so repeated setup_logging() calls don't
# leak listener threads (each owns a daemon thread draining its queue).
_queue_listeners: dict[str, logging.handlers.QueueListener] = {}


def setup_logging(name, log_dir="logs", log_filename="debug.log", *,
                  console_level=logging.INFO, file_level=logging.DEBUG,
                  silence_third_party=True, clear_file=True):
    """Configure and return a logger with non-blocking file + console handlers.

    Records are routed through a ``QueueHandler`` into a ``QueueListener``
    running on a background thread, so log emission never does disk/console
    I/O on the caller's thread (critical: the asyncio event loop must not
    block on logging in the trading hot path).

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
        If True, deletes the existing log file (and its rotated backups)
        on startup; if False the existing file is appended to.
    """
    os.makedirs(log_dir, exist_ok=True)

    # Remove any pre-existing root handlers to avoid duplicate output
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    log_path = os.path.join(log_dir, log_filename)
    if clear_file:
        for path in (log_path, *(f"{log_path}.{i}" for i in range(1, 4))):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    # Rotating handler: DEBUG-level output grows without bound on multi-day
    # runs (per-trade / per-stats INFO lines) — cap at 50MB x 3 backups.
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, mode='a', encoding='utf-8',
        maxBytes=50 * 1024 * 1024, backupCount=3,
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    # Windows consoles often use cp1252 — render unencodable chars (emoji)
    # as replacements instead of raising per-record logging errors.
    _stream = getattr(console_handler, 'stream', None)
    if hasattr(_stream, 'reconfigure'):
        try:
            _stream.reconfigure(errors='replace')
        except Exception:
            pass

    # Tear down a previous listener for this logger name (e.g. tests calling
    # setup_logging repeatedly) before replacing its handlers.
    old_listener = _queue_listeners.pop(name, None)
    if old_listener is not None:
        try:
            old_listener.stop()
        except Exception:
            pass

    log_queue: queue.SimpleQueue = queue.SimpleQueue()
    queue_handler = logging.handlers.QueueHandler(log_queue)
    listener = logging.handlers.QueueListener(
        log_queue, file_handler, console_handler, respect_handler_level=True,
    )
    listener.start()
    _queue_listeners[name] = listener
    atexit.register(_stop_listener, name)

    logger.handlers.clear()
    logger.addHandler(queue_handler)
    logger.propagate = False

    if silence_third_party:
        for lib in ('websockets', 'asyncio', 'urllib3', 'parso', 'fsspec'):
            logging.getLogger(lib).setLevel(logging.WARNING)
        logging.root.setLevel(logging.WARNING)

    return logger


def _stop_listener(name):
    """Flush and stop the queue listener for *name* (idempotent)."""
    listener = _queue_listeners.pop(name, None)
    if listener is not None:
        try:
            listener.stop()
        except Exception:
            pass


def setup_summary_logger(log_dir="logs"):
    """Console + file logger for periodic summaries (used by gather_lighter_data)."""
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "debug.log")

    file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
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
