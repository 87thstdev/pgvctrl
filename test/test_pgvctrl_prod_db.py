import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)

NO_PROD_FLG = "Production changes need the -production flag: {0}\n"
NO_PROD_FLG_APPLY = NO_PROD_FLG.format(Const.APPLY_ARG)
NO_PROD_FLG_PUSHDATA = NO_PROD_FLG.format(Const.PULL_DATA_ARG)

NO_PROD_USE = "Production changes not allowed for: {0}\n"
NO_USE_APPLYFF = "Fast forwards only allowed on empty databases.\n"
BAD_VER = "0.1.0"


class TestPgvctrlProdDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
                Const.INIT_ARG,
                Const.PRODUCTION_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
                Const.APPLY_ARG,
                Const.PRODUCTION_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
                Const.V_ARG,
                "0.0.0",
            ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(TestUtil.config_file)

    def test_chkver_no_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_first_version}.0: "
                    f"{TestUtil.pgvctrl_test_repo} PRODUCTION environment (None)\n"
        )

    def test_apply_bad_version_no_prod_flag(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    BAD_VER,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_PROD_FLG_APPLY,
                error_code=1
        )

    def test_apply_bad_version_on_production_no_prod_flag(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    BAD_VER,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_PROD_FLG_APPLY,
                error_code=1
        )

    def test_apply_bad_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    BAD_VER,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Repository version does not exist: {TestUtil.pgvctrl_test_repo} {BAD_VER}\n",
                error_code=1
        )

    def test_apply_good_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "2.0.0",
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=TestUtil.sql_return
        )

    def test_set_fast_forward(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SETFF_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Fast forward set: {TestUtil.pgvctrl_test_repo}\n"
        )

    def test_apply_fast_forward(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_FF_ARG,
                    TestUtil.test_first_version,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_USE_APPLYFF,
                error_code=1
        )


class TestPgvctrlProdPushingDb:
    def setup_method(self):
        TestUtil.get_static_data_config()
        TestUtil.get_static_error_set_data()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
                    Const.INIT_ARG,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ])

        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
                    Const.APPLY_ARG,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.V_ARG,
                    "0.0.0",
            ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{Const.PUSHING_DATA}\nRunning: error_set.sql\n\n"
        )

    def test_push_data_no_prod_flag(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_PROD_FLG_PUSHDATA,
                error_code=1
        )


class TestPgvctrlProdDbEnv:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
                Const.INIT_ARG,
                Const.PRODUCTION_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
                Const.SET_ENV_ARG,
                TestUtil.env_test,
            ])
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
                Const.APPLY_ARG,
                Const.PRODUCTION_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
                Const.ENV_ARG,
                TestUtil.env_test,
            ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(TestUtil.config_file)

    def test_chkver_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_first_version}.0: "
                    f"{TestUtil.pgvctrl_test_repo} PRODUCTION environment ("
                    f"{TestUtil.env_test})\n"
        )


# TODO: Make tests for
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay
