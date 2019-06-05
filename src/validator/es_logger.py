"""
Defines a logger for the project.
"""
import logging

# Constants TODO make those generic
DEFAULT_TZ = "+02:00"
DEFAULT_RET_VAL = 0
FMT = "%(asctime)s.%(msecs)03d ({0}) | [%(module)s] %(name)s | %(levelname)s | %(message)s | {1} |"
DTFMT = '%Y-%m-%d %H:%M:%S'
FILENAME = 'es_logger.py'


class ESLogger(object):
    """
    Costume Logger class, defines new Debug levels and returns
    new ESLogger object.
    """
    def __init__(self, name='es_logger', level=logging.DEBUG, fmt=FMT, dtfmt=DTFMT, filename=FILENAME):
        """
        :param name: Logger name.
        :param level: Logger debug level.
        :param fmt: Log msg format.
        :param dtfmt: Date time format.
        :param filename: Log file default name.
        """
        # Create logger
        self.debug_levels = {'fatal': 50,
                            'error': 40,
                            'warning': 30,
                            'debug': 20,
                            'info': 10}
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Create file handler and set level to debug
        try:
            self.fh = logging.FileHandler(filename=filename, mode='a')
            self.fh.setLevel(logging.DEBUG)
        except IOError as e:
            raise IOError(
                "Failed to create logger file handler for file: {0}.\nError: {1}".format(FILENAME, e))

        # Create formatter
        try:
            self.formatter = logging.Formatter(fmt=fmt.format(DEFAULT_TZ, DEFAULT_RET_VAL), datefmt=dtfmt)
        except NameError as e:
            raise NameError("Illegal format for logger.\nError: {0}".format(e))

        # Add formatter to fh
        self.fh.setFormatter(self.formatter)

        # Add fh to logger
        self.logger.addHandler(self.fh)
        add_costume_debug_levels(self.debug_levels)

    def get_es_logger(self):
        """
        :return: Returns es logger after initialization.
        """
        return self.logger


def add_costume_debug_levels(levels):
    """
    Adds new deubg levels to logging module and logger instance.
    :param levels: A dict of debug level names to numeric values.
    """
    for level_name, level_value in levels.iteritems():
        add_logging_level(level_name, level_value)


def add_logging_level(level_name, level_num, method_name=None):
    """
    Costume function that adds logging debug level to logging module or instance.
    :param level_name: A string represents the debug level name.
    :param level_num: An integer, debug level numeric value.
    :param method_name: String, the logging method wanted name.
    """
    if not method_name:
        method_name = level_name.lower()

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, *args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    print("Adding log function {}".format(level_name))

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)

