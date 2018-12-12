import os

import simplejson as json


def ensure_dir_exists(dir_name):
    if not os.path.isdir(dir_name):
        os.makedirs(dir_name)


def dir_exists(dir_name):
    return os.path.isdir(dir_name)


def make_data_file(file_name):
    if not os.path.isfile(file_name):
        with open(file_name, "w") as outfile:
            str_ = json.dumps(
                [],
                indent=4,
                sort_keys=True,
                separators=(",", ": "),
                ensure_ascii=True,
            )
            outfile.write(str_)
