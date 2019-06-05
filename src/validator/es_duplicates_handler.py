"""
Deletes all duplicate documents from elasticsearch cluster or index
"""
from __future__ import print_function

from optparse import OptionParser
from es_logger import ESLogger

import elasticsearch
import hashlib
import time
import utils


# Constants
MSG_FORMAT = "%(message)s\n"


class ESDuplicatesHandler(elasticsearch.Elasticsearch):
    """
    Search and delete duplicate documents in an elasticsearch cluster
    """
    def __init__(self, es_host, es_port, test_output_dir, all_indices=True, index_name=None,
                 hash_keys=[], reserved_index_names=None, logger_path=None):
        """
        :param es_host: Elasticsearch hostname.
        :param es_port: Elasticsearch port.
        :param test_output_dir: path to an output dir for test purposes, not a must.
        :param all_indices: If set to True (Default) searches for duplicates in all indices,
            if set to False, you need to provide an index name to deduplicate.
        :param index_name: Index name to deduplicate, only if all_indices is set to False.
        :param hash_keys: A list of keys for the hash function, MUST be fields in the elasticsearch
            index documents.
        :param reserved_index_names: A list of indices to not deduplicate docs in (for example, .kibana, .filebeat)
            or any other reserved index name.
        :param logger_path:  Path to app log file.
        """
        # Create new ESLogger
        if logger_path:
            log_instance = ESLogger(name=__name__, filename=logger_path)
        else:
            log_instance = ESLogger(name=__name__)

        self.es_logger = log_instance.get_es_logger()
        self.es_host = es_host
        self.es_port = es_port
        self.all_indices = all_indices
        self.index = index_name
        self.test_output_dir = test_output_dir
        self.dict_of_duplicate_docs = {}
        self.indices_names = set()
        self.keys_to_include_in_hash = hash_keys

        if reserved_index_names:
            self.reserved_index_names = reserved_index_names
        else:
            self.reserved_index_names = [".kibana", ".metricbeat"]

        # Initiate elasticsearch object
        super(ESDuplicateHandler, self).__init__(hosts=[self.es_host], port=self.es_port)

    def deduplicate_docs_and_test_results(self):
        """
        Delete duplicates for all indices and write output files
        for testing purposes.
        """
        self._generate_indices_names()

        utils.test_deduplication(es_duplicate_handler=self,
                                 prefix="BEFORE",
                                 output_dir=self.test_output_dir)

        self.es_logger.debug("Before deduplication --------------")
        print("Before deduplication --------------")
        time.sleep(1)

        self.deduplicate_docs()

        print("After deduplication ---------------")
        self.es_logger.debug("After deduplication ---------------")
        self.es_logger.debug("Sleeping for 3 seconds")
        time.sleep(3)

        utils.test_deduplication(es_duplicate_handler=self,
                                 prefix="AFTER",
                                 output_dir=self.test_output_dir)

    def deduplicate_docs(self):
        """
        For each index name in self.indices_names deduplicate
        it's docs.
        """
        self._generate_indices_names()
        for index_name in self.indices_names:
            self._deduplicate_index_docs(index_name)

    def _deduplicate_index_docs(self, index_name):
        """
        Deduplicate documents for a specific index.
        :param index_name: Index name to deduplicate.
        """
        print("[deduplicate_index_docs] deduplicate docs for index {0}".format(index_name))
        self.es_logger.info("[deduplicate_index_docs] deduplicate docs for index {0}".format(index_name))

        # Delete duplicates
        self._scroll_over_all_docs(index_name)
        self._loop_over_hashes_and_remove_duplicates(index_name)
        self.dict_of_duplicate_docs = {}  # Reset duplicates dic

        print("[deduplicate_index_docs] finished deleting duplicates for {0}".format(index_name))
        self.es_logger.info("[deduplicate_index_docs] finished deleting duplicates for {0}".format(index_name))

    def _generate_indices_names(self):
        """
        Sets self.indices_names as a list of all elasticsearch cluster indices.
        """
        indices = self.indices.get_alias().keys()
        indices_set = set(indices)
        sorted_indices = sorted(indices_set)

        for index_name in sorted_indices:
            for reserved_name in self.reserved_index_names:
                if index_name.startswith(reserved_name):
                    try:
                        sorted_indices.remove(index_name)
                    except ValueError as e:
                        self.es_logger.error(
                            "Failed to remove reserved index name from self.reserve_names.\nError {0}".format(e))

        self.indices_names = sorted_indices

    def _populate_dict_of_duplicate_docs(self, hits):
        """
        Adds self.dict_of_duplicate_docs hash value for duplicate docs.
        :param hits: Elasticsearch result for query, hits field.
        """
        for item in hits:
            combinded_key = ""
            for mykey in self.keys_to_include_in_hash:
                combinded_key += str(item['_source'][mykey])

            _id = item['id']
            hashval = hashlib.md5(combinded_key.encode('utf-8')).digest()
            self.dict_of_duplicate_docs.setdefault(hashval, []).append(_id)

    def _scroll_over_all_docs(self, index_name):
        """
        Scrolls over all index docs and call self._populate_dict_of_duplicate_docs
        for them.
        :param index_name: Index name to search for duplicates.
        """
        data = self.search(index=index_name, scroll='1m', body={"query": {"match_all": {}}})

        # Get the scroll ID
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        # Before scroll, process current batch of hits
        self._populate_dict_of_duplicate_docs(data['hits']['hits'])

        while scroll_size > 0:
            data = self.scroll(scroll_id=sid, scroll='2m')

            # Before scroll, process current batch of hits
            self._populate_dict_of_duplicate_docs(data['hits']['hits'])

            # Update the scroll ID
            sid = data['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(data['hits']['hits'])

    def _loop_over_hashes_and_remove_duplicates(self, index_name):
        """
        Loops over all hashes list self.dict_of_duplicate_docs, and delete
        all duplicate documents.
        :param index_name: Index name to delete docs in.
        """
        # Search trough the hash of doc values to see if any
        # duplicate hashes have bee found
        for hashval, array_of_ids in self.dict_of_duplicate_docs.items():
            if len(array_of_ids) > 1:
                # Get the document that have mapped to the current hashval
                matching_docs = self.mget(index=index_name, doc_type="doc", body={"ids": array_of_ids})

                # Remove the first doc (the one to keep)
                # Delete all the others
                all_docs = matching_docs['docs']
                docs_to_delete = matching_docs['docs'][1:]

                for doc in docs_to_delete:
                    # delete doc
                    try:
                        self.delete(index=index_name, doc_type="doc", id=str(doc['_id']))
                    except elasticsearch.exceptions.NotFoundError:
                        self.es_logger.error("Failed to delete 1 doc")

    def _get_docs(self, index_name):
        """
        Return all documents in an elasticsearch index.
        :param index_name: Index name to return its docs
        :return: List of log messages as raw log messages.
        """
        es_raw_logs = []

        data = self.search(index=index_name, scroll='1m', body={"query": {"match_all": {}}})
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        while scroll_size > 0:
            data = self.scroll(scroll_id=sid, scroll='2m')

            # Get the number of results that returned in the Last scroll
            for hit in data['hits']['hits']:
                msg = MSG_FORMAT % hit["_source"]
                es_raw_logs.append(msg)

            # Update the scroll ID
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

        return es_raw_logs








