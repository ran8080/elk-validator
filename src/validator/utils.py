"""
Provides general purpose functions and constants for the project.
"""
from __future__ import print_function


# Constants
ES_SORTED = "es_sorted.txt"
ORIG_SORTED = "orig_sorted.txt"


def test_deduplication(es_duplicate_handler, prefix, output_dir):
    """
    Writes elasticsearch indices to output files.
    :param es_duplicate_handler: ESDuplicateHandler object.
    :param prefix: String represents the output file's prefix.
    :param output_dir: Path to output dir
    """
    for index_name in es_duplicate_handler.indices_names:
        print("printing docs for index name: {0} {1} change".format(index_name, prefix))
        write_list_to_file(es_duplicate_handler._get_docs(index_name),
                           "{0}/{1}_{2}.txt".format(output_dir, index_name, prefix))


def write_list_to_file(lst, file_path):
    """
    Writes a list to an output file.
    :param lst: List object to write to file.
    :param file_path:  File path.
    """
    with open(file_path, "w") as file_obj:
        for item in lst:
            file_obj.write(item)

