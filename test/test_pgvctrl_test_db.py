import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists
from test.test_util import (
    TestUtil,
    print_cmd_error_details,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestPgvctrlTestDb:
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
        TestUtil.delete_file(TestUtil.config_file)
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
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=TestUtil.sql_return
        )

    def test_apply_good_version_twice(self):
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        out_rtn, errors = capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        assert errors is None
        assert f"Applied: {TestUtil.pgvctrl_test_repo} v {TestUtil.test_version}.1" in out_rtn

    def test_apply_good_version_lesser(self):
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
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Version is higher then applying {TestUtil.test_version} > {TestUtil.test_first_version}!\n",
                error_code=1
        )

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


class TestPgvctrlTestDbFastForward:
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


class TestPgvctrlTestPushDb:
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
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.get_static_data_config()
        TestUtil.get_static_app_error_set_data()
        TestUtil.get_static_error_set_data()

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n\n'
        )


class TestPgvctrlTestPushPrePostDb:
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
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.get_static_data_config()
        TestUtil.get_static_pre_push_sql()
        TestUtil.get_static_post_push_sql()
        TestUtil.get_static_app_error_set_data()
        TestUtil.get_static_error_set_data()

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: _pre_push.sql\n\n'
                    f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n\n'
                    f'{Const.PUSHING_DATA}\nRunning: _post_push.sql\n\n'
        )


class TestPgvctrlTestPushApplyingDb:
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
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.get_static_data_applying_config()
        TestUtil.get_static_app_error_set_data()
        TestUtil.get_static_error_set_data()

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n\n'
                    f'{Const.PUSHING_DATA}\nRunning: app_error_set.sql\n\n'
        )

    def test_push_one_data_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.TBL_ARG,
                    "app_error_set",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: app_error_set.sql\n\n'
        )


class TestPgvctrlTestSnapshotDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_snapshot_config()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
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

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_snapshot_data(self):
        assert True == True


class TestPgvctrlTestPushNoDirDb:
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
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_push_data_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"No tables found to push\n"
        )


class TestPgvctrlTestPullDb:
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
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_pull_data_no_list(self):
        arg_list = [
            Const.PULL_DATA_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]

        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg="No tables to pull!\n",
                error_code=1
        )

    def test_pull_data_bad_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PULL_DATA_ARG,
                    Const.TBL_ARG,
                    TestUtil.bad_table_name,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Pulling: {TestUtil.bad_table_name}\nSql Error: pg_dump: no matching tables were found\n\n",
                error_code=1
        )

    def test_pull_data_from_repos_table_list(self):
        arg_list = [
            Const.PULL_DATA_ARG,
            Const.TBL_ARG,
            TestUtil.error_set_table_name,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        # Puts tables in the list of data
        capture_dbvctrl_out(arg_list=arg_list)

        arg_list = [
            Const.PULL_DATA_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]

        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)
        has_error_set = TestUtil.file_contains(TestUtil.error_set_data_path, TestUtil.error_set_table_name)
        has_custom_error = TestUtil.file_contains(TestUtil.error_set_data_path, TestUtil.custom_error_message)
        has_error_set_default = TestUtil.file_contains(TestUtil.test_version_data_path, TestUtil.error_set_table_error_set_table)

        print_cmd_error_details(out_rtn, arg_list)
        assert "Pulling: error_set" in out_rtn
        assert has_error_set
        assert has_custom_error
        assert has_error_set_default
        assert errors is None


class TestPgvctrlTestCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        ensure_dir_exists(TestUtil.pgvctrl_test_db_ff_path)
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_db_ff_path)
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


class TestPgvctrlTestDbEnv:
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


class TestPgvctrlTestDbError:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.get_error_sql()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_file(TestUtil.error_sql_path)
        TestUtil.delete_file(TestUtil.error_sql_rollback_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.drop_database()

    def test_apply_version_error_no_rollback(self):
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
        assert "130.Error_rollback.sql: No such file or directory" in out_rtn

    def test_apply_version_error_good_rollback(self):
        TestUtil.get_error_rollback_good_sql()
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
        TestUtil.get_error_rollback_bad_sql()
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
        assert "AN ERROR WAS THROWN IN THE ROLLBACK SQL" in out_rtn
        assert "140.ItemsAddMore" not in out_rtn
