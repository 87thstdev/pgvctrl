import os


def get_valid_elements(path, ignore=None):
    ignore_list = [] if ignore is None else ignore

    return [
        x
        for x in os.listdir(path)
        if x not in ignore_list and not x.startswith(".")
    ]


def get_valid_sql_elements(path):
    return [x for x in os.listdir(path) if x.endswith("sql")]
