import os

from plumbum import local


class TestUtil(object):
    return_code = 0
    stdout = 1
    stderr = 2
    pgvctrl_test_repo = "pgvctrl_test"
    pgvctrl_test_temp_repo = "pgvctrl_temp_test"
    pgvctrl_test_db = "pgvctrl_test_db"
    test_version = "2.0.new_version"
    test_bad_version = "999.1.bad_version"
    test_version_path = "databases/pgvctrl_temp_test/{0}".format(test_version)
    env_test = "test"
    env_qa = "qa"
    env_prod = "prod"

    @staticmethod
    def local_pgvctrl():
        return local["pgvctrl"]

    @staticmethod
    def local_psql():
        return local["psql"]
    
    @staticmethod
    def create_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(["-c", "CREATE DATABASE {0}".format(TestUtil.pgvctrl_test_db)], retcode=0)
        rtn = psql.run(["-d", TestUtil.pgvctrl_test_db, "-c",  "DROP TABLE IF EXISTS test1;"])
        print(rtn)

    @staticmethod
    def drop_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(["-c", "DROP DATABASE IF EXISTS {0}".format(TestUtil.pgvctrl_test_db)], retcode=0)
        print(rtn)

    @staticmethod
    def delete_file(file_name):
        if os.path.isfile(file_name):
            os.remove(file_name)

    @staticmethod
    def delete_folder(dir_name):
        if os.path.isdir(dir_name):
            os.rmdir(dir_name)

    @staticmethod
    def create_config():
        pgv = TestUtil.local_pgvctrl()
        rtn = pgv.run(["-mkconf"], retcode=0)
        print(rtn)


def print_cmd_error_details(rtn, arg_list):
    print(":pgvctrl {0}".format(' '.join(arg_list)))
    print("return: {0}".format(rtn))
