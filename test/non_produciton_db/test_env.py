import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestDatabaseEnv:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"No version found!\n",
                error_code=1
        )

    def test_chkver_env(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.ENV_ARG,
            TestUtil.env_test,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_first_version}.0: "
                    f"{TestUtil.pgvctrl_test_repo} environment ("
                    f"{TestUtil.env_test})\n"
        )

    def test_apply_good_version_env(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.ENV_ARG,
                    TestUtil.env_test,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Applied: {TestUtil.pgvctrl_test_repo} v {TestUtil.test_first_version}.0\n"
        )

    def test_apply_env_not_exits(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.ENV_ARG,
                    TestUtil.env_qa,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Repository environment does not exists: {TestUtil.pgvctrl_test_repo} {TestUtil.env_qa}\n",
                error_code=1
        )

    def test_apply_env_wrong_env(self):
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.ENV_ARG,
                    TestUtil.env_qa,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Environment does not match databases environment: {TestUtil.env_qa} != {TestUtil.env_test}\n",
                error_code=1
        )
