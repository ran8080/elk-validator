"""
Defines blueprint for the ESLogsReloader object,
The module loads logs back to logstash (usually missed logs)
"""
from __future__ import print_function
from elasticsearch import Elasticsearch
from threading import Thread, Lock
from optparse import OptionParser
from es_logger import ESLogger
import socket
import sys
import os


class ESLogsReloader(Elasticsearch):
    """
    Loads missing logs to elasticsearch cluster.
    """
    def __init__(self, ls_host, ls_port, input_format="FILE", input_dir=None,
                 logger_path=None):
        """
        Initializes ESLogsReloader object.
        :param ls_host: Logstash hostname or ip.
        :param ls_port: Logstash port to connect to.
        :param input_format: Input format type, can be 'FILE', 'TEMPFILE'
        or 'STDOUT'.
        :param input_dir: Directory to load logs from.
        :param logger_path: Path to logger object.
        """
        # Create new ESLogger
        if logger_path:
            log_instance = ESLogger(name=__name__, filename=logger_path)
        else:
            log_instance=ESLogger(name=__name__)

        self.es_logger = log_instance.get_es_logger()

        self.ls_host = ls_host
        self.ls_port = ls_port
        self.input_format = input_format
        self.input_dir = input_dir
        self.deserializer = LogsDeserialzer(self.input_format, self.input_dir)
        self.logs_generator = self.deserializer.deserialize()

    def reload_logs(self):
        """
        Connects to logstash and send him logs to reload
        one by one.
        """
        self.es_logger.debug("Attempting to load logs to logstash")

        # Connect to logstash
        try:
            logstash_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            addr = (self.ls_host, self.ls_port)
            logstash_sock.connect(addr)
        except socket.error as e:
            raise socket.error("Failed to connect to logstash. Error: {0}".format(e))

        # Runs over the logs generator and line by line sends them to logstash
        try:
            for line in self.logs_generator:
                logstash_sock.sendall(line)
        except socket.error as e:
            raise socket.error("Failed to send logs to logstash. Error {0}".format(e))

        try:
            logstash_sock.close()
        except socket.error as e:
            raise socket.error("Failed to close logstash socket gracefully. Error {0}".format(e))

        self.es_logger.debug("Reloaded messages to logstash successfully!")

class LogsDeserializer(object):
    """
    Returns the wanted deserializer.
    """
    def __init__(self, input_format, input_dir=None):
        """
        Initiates the logsDeserializer object.
        :param input_format: Input format type, can be FILE, TEMPFILE or STDOUT,
            for now only FILE option is supported.
        :param input_dir: Path to input dir, needed only if we
            use FILE input format.
        """
        self.input_format = input_format
        self.input_dir = input_dir

    def deserialize(self):
        """
        :return: A callback to the deserializer function.
        """
        deserializer = self._get_deserializer()
        return deserializer

    def _get_deserializer(self):
        """
        :return: Callback to wanted deserializer function.
        """
        if self.input_format == 'FILE':
            return self._deserialize_from_file
        elif self.input_format == 'TEMPFILE':
            pass
        elif self.input_format == 'STDOUT':
            pass
        else:
            raise ValueError("Wrong input format {0}".format(self.input_format))

    def _deserialize_from_file(self):
        """
        Gets a list of log files paths and returns a generator for
        their names line by line.
        :return: Generator for their names line by line.
        """
        file_paths = self._get_file_paths()
        for path in file_paths:
            try:
                with open(path, "r") as file_obj:
                    for line in file_obj:
                        yield line
            except IOError as e:
                raise IOError("Log file {0} does not exist".format(path))

    def _get_file_paths(self):
        """
        Returns a list of full paths of diff files.
        :return:
        """
        file_paths = []
        for dir_path, dir_names, file_names in os.walk(self.input_dir):
            for file_name in file_names:
                file_paths.append(os.path.join(dir_path, file_name))

        return file_paths

