import mmap
import os
import io
import sys
from contextlib import redirect_stdout
from shutil import (
    copy2,
    rmtree)
from typing import List

import simplejson as json

from plumbum import local

import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists
from dbversioning.versionedDbShellUtil import STDOUT

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dbversioning.dbvctrl import DbVctrl, parse_args

# NOTICE:
#   Only change these constants for your postgres environment if you need to for testing.
LOCAL_HOST = "localhost"
PORT = "5432"
PG_TEST_SERVICE = "pgvctrl_test"
PG_USER = "87admin"
PG_PSW = ""

#
# [pgvctrl_test:test]
#     host=localhost
#     port=5432
#     dbname=pgvctrl_test_db

# DON'T CHANGE LINES BELOW


class TestUtil(object):
    local_host_server = LOCAL_HOST
    port = PORT
    svc = PG_TEST_SERVICE
    user = PG_USER
    psw = PG_PSW
    user_bad = "BADUSER_asdfafasdfda"
    psw_bad = "BADPSW_aslkjfdoija"
    return_code = 0
    stdout = 1
    stderr = 2
    invalid_repo_name = "/////"
    pgvctrl_test_repo = "pgvctrl_test"
    pgvctrl_bad_repo = "BAD_REPO"
    pgvctrl_test_temp_repo = "pgvctrl_temp_test"
    pgvctrl_test_temp_repo_path = "databases/pgvctrl_temp_test"
    pgvctrl_no_files_repo = "pgvctrl_no_files"
    pgvctrl_no_files_repo_path = "databases/pgvctrl_no_files"
    pgvctrl_test_db = "pgvctrl_test_db"
    pgvctrl_test_db_snapshots_path = (
        f"databases/_snapshots/{pgvctrl_test_repo}"
    )
    test_first_version = "0.0.0.first"
    test_first_version_path = (
        f"databases/{pgvctrl_test_repo}/{test_first_version}"
    )
    test_version = "2.0.0.NewVersion"
    test_version_ff_path = (
        f"databases/_fastForward/{pgvctrl_test_repo}/{test_version}.sql"
    )
    test_version_data_path = f"databases/{pgvctrl_test_repo}/data/data.json"
    test_make_version = "3.0.0.MakeNewVersion"
    test_make_version_path = (
        f"databases/{pgvctrl_test_repo}/{test_make_version}"
    )
    test_bad_version = "999.1.bad_version"
    test_version_path = f"databases/pgvctrl_temp_test/{test_version}"
    error_sql = "130.Error.sql"
    error_sql_path = f"databases/pgvctrl_test/{test_version}/{error_sql}"
    error_sql_rollback = "130.Error_rollback.sql"
    error_sql_rollback_path = (
        f"databases/pgvctrl_test/{test_version}/{error_sql_rollback}"
    )
    env_test = "test"
    env_qa = "qa"
    env_prod = "prod"
    version_table_owner = "test_owner"
    schema_membership = "membership"
    schema_public = "public"
    schema_bad = "badschemaname"
    table_membership_user_state = f"{schema_membership}.user_state"
    table_public_item = f"{schema_public}.item"
    table_bad = "badtablename"
    error_set_table_name = "error_set"
    bad_table_name = "asdeeiaoivjaiosdj"
    custom_error_message = "WHY WOULD YOU DO THAT!"
    error_set_data_folder_path = f"databases/{pgvctrl_test_repo}/data"
    error_set_data_path = f"{error_set_data_folder_path}/error_set.sql"
    config_file = "dbRepoConfig.json"
    data_file_default = "data.json.default"
    error_set_file_default = "error_set.sql.default"

    sql_return = 'Running: 100.AddUsersTable\n\n' \
                 'Running: 110.Notice\n' \
                 '\t8: NOTICE:  WHO DAT? 87admin\n' \
                 '\t8: NOTICE:  Just me, 87admin\n' \
                 '\t8: NOTICE:  Guess we are talking to ourselves!  87admin\n\n' \
                 'Running: 120.ItemTable\n\n' \
                 'Running: 140.ItemsAddMore\n\n' \
                 'Running: 200.AddEmailTable\n\n' \
                 'Running: 300.UserStateTable\n\n' \
                 'Running: 400.ErrorSet\n\n' \
                 f'Applied: {pgvctrl_test_repo} v {test_version}.0\n'

    @staticmethod
    def local_psql():
        return local["psql"]

    @staticmethod
    def create_database():
        TestUtil.drop_database()

        psql = TestUtil.local_psql()
        rtn = psql.run(
            ["-c", f"CREATE DATABASE {TestUtil.pgvctrl_test_db}"], retcode=0
        )
        print(rtn)

    @staticmethod
    def create_table_owner_role():
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-c", f"CREATE ROLE {TestUtil.version_table_owner};"],
                retcode=(0, 1)
        )
        print(rtn)

    @staticmethod
    def remove_table_owner_role():
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-c", f"DROP ROLE {TestUtil.version_table_owner};"],
                retcode=(0, 1)
        )
        print(rtn)

    @staticmethod
    def get_table_owner(db_name: str, table_name: str):
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-d",
                 db_name,
                 "-A",
                 "-c",
                 f"SELECT tableowner FROM pg_tables where tablename = '{table_name}';"],
                retcode=0
        )
        rtn_array = rtn[STDOUT].split("|")
        owner = rtn_array[0].split("\n")[1]

        print(owner)
        return owner

    @staticmethod
    def drop_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(
            ["-c", f"DROP DATABASE IF EXISTS {TestUtil.pgvctrl_test_db}"],
            retcode=0,
        )
        print(rtn)

    @staticmethod
    def remove_rev_recs(db_name: str):
        psql = TestUtil.local_psql()
        psql.run(
                ["-d",
                 db_name,
                 "-A",
                 "-c",
                 f"DELETE FROM repository_version;"],
                retcode=0
        )

    @staticmethod
    def delete_file(file_name):
        if os.path.isfile(file_name):
            os.remove(file_name)

    @staticmethod
    def delete_folder(dir_name):
        if os.path.isdir(dir_name):
            os.rmdir(dir_name)

    @staticmethod
    def delete_folder_full(dir_name):
        if os.path.isdir(dir_name):
            rmtree(dir_name)

    @staticmethod
    def create_config():
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.MKCONF_ARG])
        print(out_rtn)

    @staticmethod
    def mkrepo(repo_name):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, repo_name])
        print(out_rtn)

    @staticmethod
    def mkrepo_ver(repo_name, ver):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.REPO_ARG, repo_name, Const.MAKE_V_ARG, ver])
        print(out_rtn)

    @staticmethod
    def get_static_config():
        copy2(f"{TestUtil.config_file}.default", TestUtil.config_file)

    @staticmethod
    def get_static_data_config():
        ensure_dir_exists(TestUtil.error_set_data_folder_path)
        copy2(TestUtil.data_file_default, TestUtil.test_version_data_path)

    @staticmethod
    def get_static_error_set_data():
        ensure_dir_exists(TestUtil.error_set_data_folder_path)
        copy2(TestUtil.error_set_file_default, TestUtil.error_set_data_path)

    @staticmethod
    def get_static_bad_config():
        copy2(f"{TestUtil.config_file}.bad.default", TestUtil.config_file)

    @staticmethod
    def get_static_bad_repositories_config():
        copy2(f"{TestUtil.config_file}.bad.repositories.default", TestUtil.config_file)

    @staticmethod
    def get_static_invalid_config():
        copy2(f"{TestUtil.config_file}.badjson.default", TestUtil.config_file)

    @staticmethod
    def get_error_sql():
        copy2(f"{TestUtil.error_sql_path}.default", TestUtil.error_sql_path)

    @staticmethod
    def get_error_rollback_bad_sql():
        copy2(
            f"{TestUtil.error_sql_rollback_path}.bad.default",
            TestUtil.error_sql_rollback_path,
        )

    @staticmethod
    def get_error_rollback_good_sql():
        copy2(
            f"{TestUtil.error_sql_rollback_path}.good.default",
            TestUtil.error_sql_rollback_path,
        )

    @staticmethod
    def get_repo_dict():
        d = None
        if os.path.isfile(TestUtil.config_file):
            with open(TestUtil.config_file) as json_data:
                d = json.load(json_data)

        return d

    @staticmethod
    def file_contains(file_path, key):
        with open(file_path, "rb", 0) as file, mmap.mmap(
            file.fileno(), 0, access=mmap.ACCESS_READ
        ) as s:
            if s.find(key.encode()) != -1:
                return True

        return False


def capture_dbvctrl_out(arg_list: List[str]):
    args = parse_args(arg_list)
    out = io.StringIO()
    errors = None
    with redirect_stdout(out):
        try:
            DbVctrl.run(args)
        except BaseException as e:
            errors = e

    return out.getvalue(), errors


def print_cmd_error_details(rtn, arg_list):
    print(f":pgvctrl {' '.join(arg_list)}")
    print(f"return: {rtn}")


def dbvctrl_assert_simple_msg(arg_list: List[str], msg: str, error_code: int=None):
    out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

    print_cmd_error_details(out_rtn, arg_list)

    if error_code:
        assert errors.code == error_code
    else:
        assert errors is None

    assert out_rtn == msg
