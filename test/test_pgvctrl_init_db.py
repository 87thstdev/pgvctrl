import dbversioning.dbvctrlConst as Const
from dbversioning.repositoryconf import REPOSITORY_VERSION
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg,
    capture_dbvctrl_out)


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

    def test_init_test_twice(self):
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
                    Const.PWD_ARG,
                    TestUtil.psw,
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
                    Const.PWD_ARG,
                    TestUtil.psw,
                    Const.USER_ARG,
                    TestUtil.user
                ],
                msg="Database initialized (PRODUCTION)\n"
        )

    def test_init_test_svc(self):
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


class TestPgvctrlInitWithTblOwnerWithDb:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.remove_table_owner_role()
        TestUtil.create_table_owner_role()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)
        capture_dbvctrl_out(arg_list=[
            Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
            TestUtil.version_table_owner,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
        ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.remove_table_owner_role()
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
        owner = TestUtil.get_table_owner(
                db_name=TestUtil.pgvctrl_test_db,
                table_name=REPOSITORY_VERSION
        )
        assert owner == TestUtil.version_table_owner


class TestPgvctrlInitWithTblOwnerWithDbRoleMissing:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.remove_table_owner_role()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)
        capture_dbvctrl_out(arg_list=[
            Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
            TestUtil.version_table_owner,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
        ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.remove_table_owner_role()
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error ERROR:  role \"{TestUtil.version_table_owner}\" does not exist\n\n",
                error_code=1
        )
