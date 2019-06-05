"""
Entry point for the program.
"""
from __future__ import print_function
from es_insanity_checker import ESInsanityChecker
from es_logs_reloader import ESLogsReloader
from es_duplicates_handler import ESDuplicatesHandler
from es_logger import ESLogger
from traceback import print_exc
from optparse import OptionParser
from yaml import load, dump
from tqdm import tqdm

import time

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def get_option_parser():
    """
    Initiates the argument parser.
    :return: OptionParser object
    """
    parser = OptionParser()
    parser.add_option("-c", "--config_file", action="store", type="string", dest="config_file",
                      help="A yaml configuration file, configures program's behavior.")
    return parser


def main():
    # Parse arguments
    parser = get_option_parser()
    (options, args) = parser.parse_args()

    # Read configuration from yaml file
    with open(options.config_file, "r") as file_obj:
        data = load(file_obj)

    # Don't generate test output in duplicate handler unless the
    # option is enabled in the config file
    generate_test_output = False

    try:
        # Create logger for main module
        logger_options = data["logger"]
        logger_path = logger_options["logger_path"]

        # Create new ESLogger
        if logger_path:
            log_instance = ESLogger(name=__name__, filename=logger_path)
        else:
            log_instance = ESLogger(name=__name__)

        es_logger = log_instance.get_es_logger()

        # Initiates ESInsanityChecker object with args from yaml file
        ic_options = data["insanity_checker"]
        insanity_checker = ESInsanityChecker(es_host=ic_options["es_host"],
                                             es_port=ic_options["es_port"],
                                             output_dir=ic_options["output_dir"],
                                             logs_dir=ic_options["logs_dir"],
                                             output_format=ic_options["output_format"],
                                             logger_path=logger_path)

        # Initiates ESLogsReloader object with args from yaml file
        r_options = data["reloader"]
        reloader = ESInsanityChecker(ls_host=r_options["ls_host"],
                                     ls_port=r_options["ls_port"],
                                     input_format=r_options["input_format"],
                                     input_dir=r_options["input_dir"],
                                     logger_path=logger_path)

        # Initiates ESDuplicateHandler object with args from yaml file
        dh_options = data["duplicate_handler"]
        duplicate_handler = ESDuplicatesHandler(es_host=dh_options["es_host"],
                                                es_port=dh_options["es_port"],
                                                test_output_dir=dh_options["test_output_dir"],
                                                hash_keys=dh_options["hash_keys"],
                                                reserved_index_names=dh_options["reserved_index_names"],
                                                logger_path=logger_path)

        generate_test_output = dh_options["generate_test_output"]

    except KeyError as e:
        print_exc()
        raise KeyError("Illegal configuration, read configuration guide from pointers.")

    except ValueError as e:
        print_exc()
        raise ValueError("Illegal configuration, read configuration guide from pointers.")

    # Run general tests
    print("Running insanity checks...")
    es_logger.info("Running insanity checks...")

    insanity_checker.run_tests()

    print("Finished Insanity checks...")
    es_logger.info("Finished Insanity checks...")

    # Reloads missing logs to elasticsearch through logstash
    print("Reloading missing logs...")
    es_logger.info("Reloading missing logs...")

    reloader.reaload_logs()

    print("Waiting for reloader to finish loading docs...")
    es_logger.info("Waiting for reloader to finish loading docs...")
    for i in tqdm(range(10)):
        time.sleep(1)  # Change to 30 later

    print("Finished reloading logs...")
    es_logger.info("Finished reloading logs...")

    # Locate and delete duplicate documents in elasticsearch cluster
    print("Deduplicating docs...")
    es_logger.info("Deduplicating docs...")

    # TODO do this with multi processes
    if generate_test_output:
        duplicate_handler.deduplicate_docs_and_test_results()
    else:
        duplicate_handler.deduplicate_docs()

    print("Waiting for ESDuplicatesHandler to delete duplicates")
    es_logger.info("Waiting for ESDuplicatesHandler to delete duplicates")
    for i in tqdm(range(5)):
        time.sleep(1)

    print("Finished deduplicating docs...")
    es_logger.info("Finished deduplicating docs...")

    print("Finished validator tasks successfully...")
    es_logger.debug("Finished validator tasks successfully...")


if __name__ == '__main__':
    main()









