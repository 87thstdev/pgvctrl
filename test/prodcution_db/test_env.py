import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestProdDatabaseEnv:
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
