"""
Defines blueprint for the ESInsanityChecker object.
Handles running integrity checks on elasticsearch cluster.
Comparing directory with original logs to the corresponding content in
elasticsearch cluster and find missing documents.
"""
from __future__ import print_function

from optparse import OptionParser
from es_logger import ESLogger
from . import utils

import elasticsearch
import os
import urllib3
import threading
import re


# Constants
MKDIR_CMD = ["mkdir", "-p"]
ES_SORTED = "es_sorted.txt"
ORIG_SORTED = "orig_sorted.txt"
DIFF_FILE = "es_to_orig.diff"
MSG_FORMAT = "%(message)s\n"

# Regex Patterns
RE_PATTERN = "([12]\d{3})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]).*\|\s+\[(\w+)]\s+([\w]+)"

# Index name format
INDEX_NAME_FORMAT = "{0}-{1}-{2}.{3}.{4}"

# How many times to ask for 10,000 more indices (for now i is set to 5, hence 50,000 docs at max)
ES_DOC_SCROLL = 5

# Elasticsearch query
# Notice that we give the index maximum size of 50,000 docs, can ve raised
# or use scroll and while loop to get all docs
QUERY_BODY = {"query": {"match_all": {}}, "size": 10000, "sort": [{"_id": {"order": "asc"}}]}


class ESInsanityChecker(elasticsearch.Elasticsearch):
    """
    Perform a comparison of elasticsearch cluster's documents to original logs
    to find missing docs in elastic index for later purpose (Usually to reload missing logs)
    """
    def __init__(self, es_host, es_port, output_dir, logs_dir, search_indices_by_loglines=False,
                 output_format="FILE", logger_path=None):
        """
        Handler initialization of the ESInsanityChecker object.
        :param es_host: Elasticsearch hostname or ip.
        :param es_port: Elasticsearch port.
        :param output_dir: Directory to write output files to. Full or relational path.
        :param logs_dir: Directory to search for logs in. Full or relational path.
        :param search_indices_by_loglines: Format to determine which index we should
            compare the source log file to. As for now, only False, is enabled.
            Also, a source log file must contain log messages from the same index.
        :param output_format: Format to output results in, as for now only FILE format
            is supported.
        :param logger_path: Path to the program output log file.
        """
        # Create new ESLogger
        if logger_path:
            log_instance = ESLogger(name=__name__, filename=logger_path)
        else:
            log_instance = ESLogger(name=__name__)

        self.logger_path = logger_path
        self.es_logger = log_instance.get_es_logger()
        self.indices_to_paths = {}
        self.diffs = []
        self.diffs_mutex = threading.Lock()
        self.es_host = es_host
        self.es_port = es_port
        self.es_query_result = []
        self.search_indices_by_loglines = []
        self.output_dir = output_dir
        self.output_format = output_format
        self.logs_dir = logs_dir

        super(ESInsanityChecker, self).__init__(hosts=[self.es_host], port=self.es_port)

    def run_tests(self):
        """
        Runs integrity checks for elasticsearch cluster.
        """
        self.es_logger.debug("Running insanity checks...")

        # Generate indices names
        if self.search_indices_by_loglines:
            self._collect_indices_names_by_loglines()
        else:
            self._collect_indices_names_by_filenames()

        print(self.indices_to_paths)

        # Create all diff files
        self._generate_diff_lst()

        # Generates output of the wanted type (FILE / TEMPFILE / STDOUT etc.)
        self._create_diffs()

    def _create_diffs(self):
        """
        Generates a new diff file for each index in self.indices_to_paths.
        Differentiates between Elasticsearch content and original log file.
        """
        self.es_logger.debug("Creating workers to generate diffs")

        # For each index, create a thread to calculate the diff,
        # the wait for the threads. Each thread call self._create_index_diff
        workers = []
        for key, value in self.indices_to_paths.iteritems():
            # Create thread for each key and run the function,
            # Wait in the end
            th = threading.Thread(target=self._create_index_diff, kwargs={"index_name": key, "log_file_path": value})
            th.daemon = True
            workers.append(th)

            # Start thread
            th.start()

        # Wait for workers to finish
        for worker in workers:
            worker.join()

        self.es_logger.debug("Joined workers successfully!")

    def _create_index_diff(self, index_name, log_file_path):
        """
        Creates a LogsDiff object for a given index, and appends it to self.diffs.
        The diff compares the index docs and the original log file.
        :param index_name: Elasticsearch index name as a string to create diff from.
        :param log_file_path: Path to the corresponding log file to create diff from.
        """
        es_raw_logs = self._get_es_raw_messages(index_name, QUERY_BODY)

        with open(log_file_path, "r") as file_obj:
            log_file_content = file_obj.readlines()

        # Convert dos files to unix
        log_file_content = [line.replace('\r\n', '\n') for line in log_file_content]
        es_raw_logs = [line.replace('\r\n', '\n') for line in es_raw_logs]

        # Create diff file complexity of O(n * m)
        # Length of original log and elasticsearch content
        es_set = set(es_raw_logs)
        orig_set = set(log_file_content)
        diff_lst = [line for line in orig_set if line not in es_set]

        # Returns diff object
        diff_obj = LogsDiff(diff_name=index_name, log_lines=diff_lst, output_dir=self.output_dir)

        # Take ownership of mutex and append diff object to diff list
        with self.diffs_mutex:
            self.diffs.append(diff_obj)

    def _generate_index_name_by_filename(self, filename):
        """
        Generates an index from a given filename.
        :param filename: Filename to get index name from.
        :return: The index name, str.
        """
        with open(filename, "r") as file_obj:
            line = file_obj.readline()

        # Filter index name with regex from log line
        result = re.findall(RE_PATTERN, line)[0]
        if not result:
            self.es_logger.error("Error, couldn't filter logline with regex. line {0}".format(line))

        # TODO make this generic
        year = result[0]
        month = result[1]
        day = result[2]
        system = result[3]
        entity = result[4]
        index_name = INDEX_NAME_FORMAT.format(system, entity, year, month, day)
        return index_name

    def _generate_index_name_by_loglines(self, log_line):
        """
        Generates an index name from a given filename.
        :param log_line: A string representing the log lien to process.
        :return: Returns the index name as a string.
        """
        pass

    def _collect_indices_names_by_filenames(self):
        """
        Runs over the logs dir, and for each log file
        generates an index name based on the first line of the file (Should be a log line),
        then it appends the names to self.indices_to_paths.
        """
        # TODO redesign this function
        # When saving the indices names, save as value in the dict hte diff files path
        for filename in os.listdir(self.logs_dir):
            if not os.path.isfile(os.path.join(self.logs_dir, filename)):
                self.es_logger.error("Entry point is not a regular file: {0}".format(filename))
                continue

            file_path = os.path.join(self.logs_dir, filename)
            index_name = self._generate_index_name_by_filename(file_path)
            if not index_name in self.indices_to_paths:
                # If index name is not in self.indices_to_paths, append its value is filename
                self.indices_to_paths[index_name] = file_path

    def _collect_indices_names_by_loglines(self):
        """
        Runs over the logs dir, and for each log file,
        for each line in the file, generates an index name based
        on the line's content. Then it appends the names to self.indices_to_paths.
        """
        for root, dirs, files in os.walk(self.logs_dir):
            for filename in files:
                # TODO run search in each file as a single process
                # TODO Have a mutex on the dictionary
                pass

    def _get_es_raw_messages(self, index_name, query_body):
        """
        Query elasticsearch for index content, and filter
        the 'messages' field from each document.
        :param index_name: The index name as a str.
        :param query_body: A string of the query body.
        :return: Returns a list of all messages under the
            index, in raw format.
        """
        es_raw_logs = []

        # Query Elasticsearch
        # TODO handle result not returning from ES (If the index doesn't exist)
        try:
            res = super(ESInsanityChecker, self).search(index=index_name, body=query_body, scroll='1m')
            scroll_id = res['_scroll_id']

            # Use elasticsearch Scroll API to get more than 10,000 docs
            # Runs for as many iterations ES_DOC_SCROLL defined to
            for i in xrange(0, ES_DOC_SCROLL):
                print("Index {0}: Got {1} hits".format(index_name, res['hits']['total']))
                for hit in res['hits']['hits']:
                    es_raw_logs.append(MSG_FORMAT % hit["_source"])

                scroll_id = res['_scroll_id']
                res = super(ESInsanityChecker, self).scroll(scroll_id=scroll_id, scroll='1m')

        except urllib3.connection.ConnectionError as e:
            raise urllib3.connection.ConnectionError("Failed to connect to elasticsearch host")

        return es_raw_logs


class LogsDiff(object):
    """
    Logs diff object, has a name and a list of log lines.
    """
    def __init__(self, diff_name, log_lines, output_dir):
        """
        Initiates LogsDiff object.
        :param diff_name: A name for the LogsDiff object.
        :param log_lines: List that contains the lines of the diff.
        :param output_dir: Output dif to write diff to.
        """
        self.diff_name = diff_name
        self.log_lines = log_lines
        self.output_dir = output_dir


class DiffSerializer(object):
    """
    Diff object serializer, provides an interface
    for several output serializers, utilizes the Factory Design pattern.
    """
    def __init__(self, diff, output_format, output_dir=None, logger_path=None):
        """
        Initiates serializer.
        :param diff: Diff object contains the output lines.
        :param output_format: Output format string, for now only FILE format is supported.
        :param output_dir: Output dir's for path.
        :param logger_path: Path to logger output file.
        """
        # Create new ESLogger
        if logger_path:
            log_instance = ESLogger(name=__name__, filename=logger_path)
        else:
            log_instance = ESLogger(name=__name__)

        self.es_logger = log_instance.get_es_logger()
        self.diff = diff
        self.output_format = output_format
        self.output_dir = output_dir

    def serialize(self):
        """
        Serializes the output according to the output format.
        """
        serializer = self._get_serializer(self.output_format)
        serializer(self.diff)

    def _get_serializer(self, output_format):
        """
        Returns a serializer function for each output_format.
        Output format can be FILE, TEMPFILE or STDOUT
        :param output_format:
        :return: Serializer function callback.
        """
        if output_format == 'FILE':
            return self._serialze_to
        elif output_format == 'TEMPFILE':
            return self._serialize_to_tempfile
        elif output_format == 'STDOUT':
            return self._serialize_to_stdout
        else:
            raise ValueError("Format {} is not supported".format(output_format))

    def _serialize_to_file(self, diff):
        """
        Generate output diff file to path, create output dirs and diff file.
        :param diff: diff object to write to file.
        """
        self._create_index_dir(self.diff.diff_name)
        diff_path = os.path.join(self.output_dir, self.diff.diff_name, DIFF_FILE)
        utils.write_list_to_file(self.diff.log_lines, diff_path)

    def _serialize_to_tempfile(self, diff):
        """

        :param diff:
        :return:
        """
        pass

    def _serialize_to_stdout(self, diff):
        """

        :param diff:
        :return:
        """
        pass

    def _write_diff_file(self):
        """
        :return:
        """
        pass

    def _create_index_dir(self, diff_name):
        """
        Create a new directory for the given index.
        :param diff_name: The diff file name, as a string.
        """
        dir_path = os.path.join(self.output_dir, diff_name)
        if not os.path.isdir(dir_path):
            self.es_logger.debug(
                "Creating directory for index {0} in path: {1}".format(diff_name, dir_path))
            try:
                os.makedirs(dir_path)
            except OSError as e:
                self.es_logger.error("Failed to create dir {0}. Error: {1}".format(e))
        else:
            self.es_logger.warning(
                "Failed to create directory {0}, path already exists.".format(dir_path))
