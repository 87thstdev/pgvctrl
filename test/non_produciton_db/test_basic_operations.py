import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg, print_cmd_error_details)


class TestBasicDatabaseOperation:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.drop_database()
        TestUtil.create_database()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            TestUtil.pgvctrl_test_repo
        ])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.mkrepo_ver(
            repo_name=TestUtil.pgvctrl_test_repo, version=TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_first_version}.0: {TestUtil.pgvctrl_test_repo} environment (None)\n"
        )

    def test_chkver_no_recs(self):
        TestUtil.remove_rev_recs(TestUtil.pgvctrl_test_db)

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

    def test_chkver_too_many_recs(self):
        TestUtil.add_rev_recs(TestUtil.pgvctrl_test_db)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Too many version records found!\n",
                error_code=1
        )

    def test_apply_bad_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "0.1.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Repository version does not exist: {TestUtil.pgvctrl_test_repo} 0.1.0\n",
                error_code=1
        )

    def test_apply_good_version(self):
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        assert out_rtn == 'Applied: pgvctrl_test v 0.0.0.first.1\n'
        assert errors is None

    def test_apply_good_version_bad_sql_name(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version="1.0.0",
                file_name=TestUtil.bad_sql_name
        )

        out_rtn, errors = capture_dbvctrl_out(arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "1.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ])

        assert errors.code == 1
        assert out_rtn.startswith("Sql filename error:")
        assert TestUtil.bad_sql_name in out_rtn

    def test_apply_good_version_twice(self):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        assert f"Applied: {TestUtil.pgvctrl_test_repo} v {TestUtil.test_first_version}.1" in out_rtn
        assert errors is None

    def test_apply_good_version_lesser(self):
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        assert out_rtn == f"Version is higher then applying {TestUtil.test_version} > {TestUtil.test_first_version}!\n"
        assert errors.code == 1

    def test_apply_good_version_passing_v(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.ENV_ARG,
                    TestUtil.env_test,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Version and environment args are mutually exclusive.\n",
                error_code=1
        )


class TestApplyRollback:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.create_database()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            "pgvctrl_test",
            Const.DATABASE_ARG,
            "pgvctrl_test_db",
        ])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_apply_version_error_no_rollback(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="130.Error.sql",
                contents="DO $$ BEGIN RAISE EXCEPTION 'AN ERROR WAS THROWN IN SQL'; END $$;"
        )

        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors.code == 1
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback.sql: No such file or directory" in out_rtn

    def test_apply_version_error_good_rollback(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="140.ItemsAddMore.sql",
                contents="SELECT 'ItemsAddMore' AS action;"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="130.Error.sql",
                contents="DO $$ BEGIN RAISE EXCEPTION 'AN ERROR WAS THROWN IN SQL'; END $$;"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="130.Error_rollback.sql",
                contents="SELECT 'rolledback' AS action;"
        )

        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback" in out_rtn
        assert "140.ItemsAddMore" in out_rtn

    def test_apply_version_error_bad_rollback(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="130.Error.sql",
                contents="DO $$ BEGIN RAISE EXCEPTION 'AN ERROR WAS THROWN IN SQL'; END $$;"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="130.Error_rollback.sql",
                contents="DO $$ BEGIN RAISE EXCEPTION 'AN ERROR WAS THROWN IN SQL'; END $$;"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="140.ItemsAddMore.sql",
                contents="SELECT 'ItemsAddMore' AS action;"
        )

        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors.code == 1
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback" in out_rtn
        assert "140.ItemsAddMore" not in out_rtn
