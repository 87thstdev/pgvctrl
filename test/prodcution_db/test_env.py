import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestProdDatabaseEnv:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.mkrepo(
                repo_name=TestUtil.pgvctrl_test_repo
        )
        TestUtil.mkrepo_ver(
                repo_name=TestUtil.pgvctrl_test_repo, version=TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.MAKE_ENV_ARG,
            TestUtil.env_test,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
            Const.V_ARG,
            TestUtil.test_first_version
        ])
        TestUtil.drop_database()
        TestUtil.create_database()
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
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

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
