import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestPgvctrlInitWithOutDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init_invalid(self):
        arg_list = [
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Invalid Data Connection: ['{Const.DATABASE_ARG}', '{TestUtil.pgvctrl_test_db}']\n",
                error_code=1
        )

    def test_init_invalid_db_conn(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                ],
                msg=f"Missing connection args\n",
                error_code=1
        )


class TestPgvctrlInitWithDb:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Database initialized\n"
        )

    def test_init_production(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Database initialized (PRODUCTION)\n"
        )

    def test_init_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.SET_ENV_ARG,
                    TestUtil.env_test,
                ],
                msg=f"Database initialized environment ({TestUtil.env_test})\n"
        )

    def test_init_production_with_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.SET_ENV_ARG,
                    TestUtil.env_test,
                ],
                msg=f"Database initialized (PRODUCTION) environment ({TestUtil.env_test})\n"
        )
