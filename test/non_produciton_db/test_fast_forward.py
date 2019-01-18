import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestDatabaseFastForward:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
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
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ff_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

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

    def test_set_fast_forward_include_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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
        has_member_sch = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_public_sch = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE SCHEMA {TestUtil.schema_public}",
        )
        assert has_member_sch is True
        assert has_public_sch is False

    def test_set_fast_forward_include_schema_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_bad,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SETFF_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error pg_dump: no matching schemas were found\n\n",
                error_code=1
        )

    def test_set_fast_forward_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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

        has_member = TestUtil.file_contains(
            TestUtil.test_version_ff_path, TestUtil.schema_membership
        )
        has_public = TestUtil.file_contains(
            TestUtil.test_version_ff_path, TestUtil.schema_public
        )
        assert has_member is False
        assert has_public is True

    def test_set_fast_forward_exclude_schema_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_bad,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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

    def test_set_fast_forward_include_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_public,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SETFF_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo} cannot have both included and excluded schemas!\n",
                error_code=1
        )

    def test_set_fast_forward_include_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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
        has_member_tbl = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        has_public_tbl = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE TABLE {TestUtil.table_public_item}",
        )
        assert has_member_tbl is True
        assert has_public_tbl is False

    def test_set_fast_forward_include_table_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_ARG,
            TestUtil.table_bad,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SETFF_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error pg_dump: no matching tables were found\n\n",
                error_code=1
        )

    def test_set_fast_forward_exclude_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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

        has_member = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        has_public = TestUtil.file_contains(
            TestUtil.test_version_ff_path, TestUtil.table_public_item
        )
        assert has_member is False
        assert has_public is True

    def test_set_fast_forward_exclude_table_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_ARG,
            TestUtil.table_bad,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

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

    def test_set_fast_forward_include_schema_exclude_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

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
        has_member_sch = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_member_tbl = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        assert has_member_sch is True
        assert has_member_tbl is False

    def test_set_fast_forward_exclude_schema_include_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

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
        has_member_sch = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_member_tbl = TestUtil.file_contains(
            TestUtil.test_version_ff_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        assert has_member_sch is False
        assert has_member_tbl is True

    def test_apply_fast_forward_fail(self):
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

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_FF_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Fast forwards only allowed on empty databases.\n",
                error_code=1
        )

    def test_apply_fast_forward(self):
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

        TestUtil.drop_database()
        TestUtil.create_database()

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_FF_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Applying: {TestUtil.test_first_version}\n\n"
        )

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


class TestFastForwardOnCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        ensure_dir_exists(TestUtil.pgvctrl_test_db_ff_path)
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ff_path)
        TestUtil.drop_database()

    def test_apply_bad_fast_forward(self):
        bad_ff = "BAD"

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_FF_ARG,
                    bad_ff,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Fast forward not found {bad_ff}\n",
                error_code=1
        )

    def test_apply_fast_forward_bad_db(self):
        good_ff = "0.0.0.gettingStarted"
        bad_db = "asdfasdfasdfasdfa123123asdfa"

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_FF_ARG,
                    good_ff,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    bad_db,
                ],
                msg=f"Invalid Data Connection: ['{Const.DATABASE_ARG}', '{bad_db}']\n",
                error_code=1
        )
