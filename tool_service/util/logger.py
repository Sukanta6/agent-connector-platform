import logging
import os
from typing import List


class MemoryHandler(logging.Handler):
    """Custom handler that collects logs in memory."""
    
    def __init__(self):
        super().__init__()
        self.logs: List[str] = []
    
    def emit(self, record):
        """Store log record in memory."""
        self.logs.append(self.format(record))
    
    def get_logs(self) -> List[str]:
        """Get all collected logs."""
        return self.logs
    
    def clear(self):
        """Clear all logs."""
        self.logs.clear()


# Global memory handler to collect logs
_memory_handler = MemoryHandler()


def get_logger(name: str) -> logging.Logger:
    """Get logger that collects logs in memory."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # Console handler (optional - for live viewing)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Memory handler - collects logs
        _memory_handler.setFormatter(formatter)
        logger.addHandler(_memory_handler)

    return logger


def save_logs_to_file(log_file: str = "logs/app.log") -> None:
    """Save all collected logs to file at the end."""
    logs = _memory_handler.get_logs()
    
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with open(log_file, 'a', encoding='utf-8') as f:
        for log in logs:
            f.write(log + '\n')


def clear_logs() -> None:
    """Clear all collected logs from memory."""
    _memory_handler.clear()