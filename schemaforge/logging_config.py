"""
SchemaForge Logging Configuration

Provides structured logging with support for:
- Console output with colors
- JSON format for production/CI environments
- Configurable log levels
"""

import logging
import json
import sys
from datetime import datetime
from typing import Optional


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for machine parsing."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, 'table_name'):
            log_entry["table_name"] = record.table_name
        if hasattr(record, 'operation'):
            log_entry["operation"] = record.operation
        if hasattr(record, 'dialect'):
            log_entry["dialect"] = record.dialect
        if hasattr(record, 'file_path'):
            log_entry["file_path"] = record.file_path
            
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry)


class ColoredFormatter(logging.Formatter):
    """Format log records with ANSI colors for console output."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def __init__(self, use_color: bool = True):
        super().__init__()
        self.use_color = use_color
        
    def format(self, record: logging.LogRecord) -> str:
        level = record.levelname
        message = record.getMessage()
        
        # Build context string from extra fields
        context_parts = []
        if hasattr(record, 'table_name'):
            context_parts.append(f"table={record.table_name}")
        if hasattr(record, 'operation'):
            context_parts.append(f"op={record.operation}")
        if hasattr(record, 'dialect'):
            context_parts.append(f"dialect={record.dialect}")
            
        context_str = f" [{', '.join(context_parts)}]" if context_parts else ""
        
        if self.use_color and level in self.COLORS:
            return f"{self.COLORS[level]}[{level}]{self.RESET} {message}{context_str}"
        else:
            return f"[{level}] {message}{context_str}"


def setup_logging(
    verbose: int = 0,
    log_format: str = "text",
    no_color: bool = False
) -> logging.Logger:
    """
    Configure and return the SchemaForge logger.
    
    Args:
        verbose: Verbosity level (0=WARNING, 1=INFO, 2+=DEBUG)
        log_format: Output format - "text" or "json"
        no_color: Disable ANSI colors in text output
        
    Returns:
        Configured logger instance
    """
    # Determine log level based on verbosity
    if verbose >= 2:
        level = logging.DEBUG
    elif verbose == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
        
    # Get or create logger
    logger = logging.getLogger("schemaforge")
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create handler
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    
    # Set formatter based on format type
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        use_color = not no_color and sys.stderr.isatty()
        formatter = ColoredFormatter(use_color=use_color)
        
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance for a module.
    
    Args:
        name: Optional module name to append to 'schemaforge'
        
    Returns:
        Logger instance
    """
    if name:
        return logging.getLogger(f"schemaforge.{name}")
    return logging.getLogger("schemaforge")
