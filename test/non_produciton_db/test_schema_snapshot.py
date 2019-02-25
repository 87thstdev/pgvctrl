from datetime import datetime

import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists
from dbversioning.versionedDbShellUtil import SNAPSHOT_DATE_FORMAT
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestDatabaseSchemaSnapshot:
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
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ss_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

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

    def test_set_schema_snapshot_include_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
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
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"

        has_member_sch = TestUtil.file_contains(
            full_path,
            f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_public_sch = TestUtil.file_contains(
            full_path,
            f"CREATE SCHEMA {TestUtil.schema_public}",
        )
        assert has_member_sch is True
        assert has_public_sch is False
        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"

    def test_set_schema_snapshot_include_schema_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
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
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error pg_dump: no matching schemas were found\n\n",
                error_code=1
        )

    def test_set_schema_snapshot_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
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

        msg, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
        )
        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)
        file_name = files[0]
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"
        has_member = TestUtil.file_contains(
            full_path, TestUtil.schema_membership
        )
        has_public = TestUtil.file_contains(
            full_path, TestUtil.schema_public
        )
        date_str = file_name.split(".")[4]
        date_of = datetime.strptime(date_str, SNAPSHOT_DATE_FORMAT)

        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"
        assert type(date_of) is datetime
        assert has_member is False
        assert has_public is True

    def test_set_schema_snapshot_exclude_schema_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
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

        msg, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
        )

        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)
        file_name = files[0]
        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"

    def test_set_schema_snapshot_include_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_public,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
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
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo} cannot have both included and excluded schemas!\n",
                error_code=1
        )

    def test_set_schema_snapshot_include_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_LONG_ARG,
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

        msg, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
        )

        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)
        file_name = files[0]
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"

        has_member_tbl = TestUtil.file_contains(
            full_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        has_public_tbl = TestUtil.file_contains(
            full_path,
            f"CREATE TABLE {TestUtil.table_public_item}",
        )
        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"
        assert has_member_tbl is True
        assert has_public_tbl is False

    def test_set_schema_snapshot_include_table_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_LONG_ARG,
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
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error pg_dump: no matching tables were found\n\n",
                error_code=1
        )

    def test_set_schema_snapshot_exclude_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_LONG_ARG,
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
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"

        has_member = TestUtil.file_contains(full_path, f"CREATE TABLE {TestUtil.table_membership_user_state}")
        has_public = TestUtil.file_contains(full_path, TestUtil.table_public_item)

        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"
        assert has_member is False
        assert has_public is True

    def test_set_schema_snapshot_exclude_table_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_LONG_ARG,
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

    def test_set_schema_snapshot_include_schema_exclude_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
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
            Const.EXCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

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
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"

        has_member_sch = TestUtil.file_contains(full_path, f"CREATE SCHEMA {TestUtil.schema_membership}")
        has_member_tbl = TestUtil.file_contains(
            full_path,
            f"CREATE TABLE {TestUtil.table_membership_user_state}",
        )
        assert has_member_sch is True
        assert has_member_tbl is False
        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"
        assert errors is None

    def test_set_schema_snapshot_exclude_schema_include_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
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
            Const.INCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

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
        full_path = f"databases/_schemaSnapshot/{TestUtil.pgvctrl_test_repo}/{file_name}"

        has_member_sch = TestUtil.file_contains(full_path, f"CREATE SCHEMA {TestUtil.schema_membership}")
        has_member_tbl = TestUtil.file_contains(full_path, f"CREATE TABLE {TestUtil.table_membership_user_state}"
                                                )
        assert has_member_sch is False
        assert has_member_tbl is True
        assert msg == f"Schema snapshot set: {TestUtil.pgvctrl_test_repo} ({file_name})\n"

    def test_apply_schema_snapshot_fail(self):
        capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Schema Snapshots only allowed on empty databases.\n",
                error_code=1
        )

    def test_apply_schema_snapshot(self):
        capture_dbvctrl_out(
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

        TestUtil.drop_database()
        TestUtil.create_database()

        f_nm = file_name.rstrip('.sql')
        msg, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    f_nm,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        assert msg == f"Applying: {f_nm}\n\n"
        assert errors is None

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


class TestDatabaseSchemaSnapshotEnv:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])
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

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ss_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_set_schema_snapshot_env(self):
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


class TestSchemaSnapshotOnCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        ensure_dir_exists(TestUtil.pgvctrl_test_db_ss_path)
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ss_path)
        TestUtil.drop_database()

    def test_apply_bad_schema_snapshot(self):
        bad_ss = "BAD"

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    bad_ss,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Schema snapshot not found {bad_ss}\n",
                error_code=1
        )

    def test_apply_schema_snapshot_bad_db(self):
        good_ss = "0.0.0.gettingStarted"
        bad_db = "asdfasdfasdfasdfa123123asdfa"

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    good_ss,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    bad_db,
                ],
                msg=f"Invalid Data Connection: ['{Const.DATABASE_ARG}', '{bad_db}']\n",
                error_code=1
        )
