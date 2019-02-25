import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)

NO_PROD_FLG_APPLY = f"Production changes need the -production flag: {Const.APPLY_ARG}\n"
NO_USE_APPLYSS = "Schema Snapshots only allowed on empty databases.\n"
BAD_VER = "0.1.0"


class TestBasicProductionDatabaseOperation:
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
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ss_path)
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

    def test_set_schema_snapshot(self):
        msg, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )
        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)
        file_name = files[0]

        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"
        assert errors is None

    def test_apply_schema_snapshot(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    TestUtil.test_first_version,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_USE_APPLYSS,
                error_code=1
        )
