import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg,
    capture_dbvctrl_out)


class TestInitDb:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

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

    def test_init_svc(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.SERVICE_ARG,
                    TestUtil.svc,
                ],
                msg="Database initialized\n"
        )

    def test_init_setenv(self):
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

    def test_init_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.ENV_ARG,
                    TestUtil.env_test,
                ],
                msg=f"Error setting env: Used {Const.ENV_ARG}, did you mean {Const.SET_ENV_ARG}?\n",
                error_code=1
        )

    def test_init_twice(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Database already initialized!\n",
                error_code=1
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

    def test_init_production_bad_db(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.PRODUCTION_ARG,
                ],
                msg="Missing connection args\n",
                error_code=1
        )

    def test_init_production_long_bad_user(self):
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.PRODUCTION_ARG,
                    Const.HOST_ARG,
                    TestUtil.local_host_server,
                    Const.PORT_ARG,
                    TestUtil.port,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.USER_ARG,
                    TestUtil.user_bad
                ])

        assert errors.code == 1
        assert "Invalid Data Connection" in out_rtn

    def test_init_production_long(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.PRODUCTION_ARG,
                    Const.HOST_ARG,
                    TestUtil.local_host_server,
                    Const.PORT_ARG,
                    TestUtil.port,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.USER_ARG,
                    TestUtil.user
                ],
                msg="Database initialized (PRODUCTION)\n"
        )

    def test_init_production_with_setenv(self):
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
