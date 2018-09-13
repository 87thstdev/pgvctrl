import dbversioning.dbvctrlConst as Const
from test.test_pgvctrl_config import DB_REPO_CONFIG_JSON
from test.test_util import (
    TestUtil,
    print_cmd_error_details)


class TestPgvctrInitlWithOutDb:
    def setup_method(self, test_method):
        TestUtil.drop_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init_invalid(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.INIT_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_temp_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f"Invalid Data Connection: ['{Const.DATABASE_ARG}', " \
                                       f"'{TestUtil.pgvctrl_test_db}']\n"
        assert rtn[TestUtil.return_code] == 1


class TestPgvctrInitlWithDb:
    def setup_method(self, test_method):
        TestUtil.create_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self, test_method):
        TestUtil.drop_database()
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.INIT_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_temp_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Database initialized\n'
        assert rtn[TestUtil.return_code] == 0

    def test_init_production(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.INIT_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_temp_repo, Const.PRODUCTION_ARG,  Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Database initialized (PRODUCTION)\n'
        assert rtn[TestUtil.return_code] == 0

    def test_init_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.INIT_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_temp_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db, Const.SET_ENV_ARG,
                    TestUtil.env_test]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Database initialized environment ({TestUtil.env_test})\n'
        assert rtn[TestUtil.return_code] == 0

    def test_init_production_with_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.INIT_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_temp_repo, Const.PRODUCTION_ARG,  Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db, Const.SET_ENV_ARG, TestUtil.env_test]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Database initialized (PRODUCTION) environment ({TestUtil.env_test})\n'
        assert rtn[TestUtil.return_code] == 0
