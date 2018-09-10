from dbversioning.dbvctrl import (
    parse_args,
    INCLUDE_SCHEMA_ARG,
    EXCLUDE_SCHEMA_ARG
)
from dbversioning.dbvctrlConst import (
    INCLUDE_TABLE_ARG,
    EXCLUDE_TABLE_ARG)
from test.test_pgvctrl_config import DB_REPO_CONFIG_JSON
from test.test_util import (
    TestUtil,
    print_cmd_error_details,
    capture_dbvctrl_out)


class TestPgvctrlTestDb:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)
        pgv.run(["-apply", "-v", TestUtil.test_first_version, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db],
                retcode=0)

    def teardown_method(self, test_method):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{TestUtil.test_first_version}.0: {TestUtil.pgvctrl_test_repo} environment (' \
                                       f'None)\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "0.1.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Repository version does not exist: {0} 0.1.0\n'.format(
                TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 1

    def test_apply_good_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == TestUtil.sql_return

    def test_set_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert rtn[TestUtil.return_code] == 0

    def test_set_fast_forward_include_schema(self):
        arg_list = [INCLUDE_SCHEMA_ARG, TestUtil.schema_membership, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        has_member_sch = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE SCHEMA {TestUtil.schema_membership}")
        has_public_sch = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE SCHEMA {TestUtil.schema_public}")
        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member_sch is True
        assert has_public_sch is False

    def test_set_fast_forward_include_schema_bad(self):
        arg_list = [INCLUDE_SCHEMA_ARG, TestUtil.schema_bad, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'DB Error pg_dump: no matching schemas were found\n\n'
        assert errors.code == 1

    def test_set_fast_forward_exclude_schema(self):
        arg_list = [EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        has_member = TestUtil.file_contains(TestUtil.test_version_ff_path, TestUtil.schema_membership)
        has_public = TestUtil.file_contains(TestUtil.test_version_ff_path, TestUtil.schema_public)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member is False
        assert has_public is True

    def test_set_fast_forward_exclude_schema_bad(self):
        arg_list = [EXCLUDE_SCHEMA_ARG, TestUtil.schema_bad, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        print_cmd_error_details(out_rtn, arg_list)

        # Excluding schemas that don't exist is not an issue
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None

    def test_set_fast_forward_include_exclude_schema(self):
        arg_list = [INCLUDE_SCHEMA_ARG, TestUtil.schema_public, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'{TestUtil.pgvctrl_test_repo} cannot have both included and excluded schemas!\n'
        assert errors.code == 1

    def test_set_fast_forward_include_table(self):
        arg_list = [INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        has_member_tbl = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE TABLE {TestUtil.table_membership_user_state}")
        has_public_tbl = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE TABLE {TestUtil.table_public_item}")
        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member_tbl is True
        assert has_public_tbl is False

    def test_set_fast_forward_include_table_bad(self):
        arg_list = [INCLUDE_TABLE_ARG, TestUtil.table_bad, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'DB Error pg_dump: no matching tables were found\n\n'
        assert errors.code == 1

    def test_set_fast_forward_exclude_table(self):
        arg_list = [EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        has_member = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE TABLE {TestUtil.table_membership_user_state}")
        has_public = TestUtil.file_contains(TestUtil.test_version_ff_path, TestUtil.table_public_item)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member is False
        assert has_public is True

    def test_set_fast_forward_exclude_table_bad(self):
        arg_list = [EXCLUDE_TABLE_ARG, TestUtil.table_bad, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        print_cmd_error_details(out_rtn, arg_list)

        # Excluding tables that don't exist is not an issue
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None

    def test_set_fast_forward_include_schema_exclude_table(self):
        arg_list = [INCLUDE_SCHEMA_ARG, TestUtil.schema_membership, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        has_member_sch = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE SCHEMA {TestUtil.schema_membership}")
        has_member_tbl = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE TABLE {TestUtil.table_membership_user_state}")

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member_sch is True
        assert has_member_tbl is False

    def test_set_fast_forward_exclude_schema_include_table(self):
        arg_list = [EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)
        has_member_sch = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE SCHEMA {TestUtil.schema_membership}")
        has_member_tbl = TestUtil.file_contains(
                TestUtil.test_version_ff_path,
                f"CREATE TABLE {TestUtil.table_membership_user_state}")

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert errors is None
        assert has_member_sch is False
        assert has_member_tbl is True

    def test_apply_fast_forward_fail(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-applyff", TestUtil.test_first_version, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Fast forwards only allowed on empty databases.\n'
        assert rtn[TestUtil.return_code] == 1

    def test_push_data(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-pushdata", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Running: error_set.sql\n\n'
        assert rtn[TestUtil.return_code] == 0


# TODO: Make tests for
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay


class TestPgvctrlTestCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.drop_database()

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-applyff", TestUtil.test_first_version,  "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Applying: {TestUtil.test_first_version}\n\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()
        bad_ff = "BAD"

        arg_list = ["-applyff", bad_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Fast forward not found {0}\n'.format(bad_ff)
        assert rtn[TestUtil.return_code] == 1

    def test_apply_fast_forward_bad_db(self):
        pgv = TestUtil.local_pgvctrl()
        good_ff = "0.0.0.gettingStarted"
        bad_db = "asdfasdfasdfasdfa123123asdfa"

        arg_list = ["-applyff", good_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", bad_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Invalid Data Connection: ['-d', '{0}']\n".format(bad_db)
        assert rtn[TestUtil.return_code] == 1


class TestPgvctrlTestDbEnv:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        pgv = TestUtil.local_pgvctrl()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)
        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'No version found!\n'
        assert rtn[TestUtil.return_code] == 1

    def test_chkver_env(self):
        pgv = TestUtil.local_pgvctrl()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db,
                 "-setenv", TestUtil.env_test], retcode=0)
        pgv.run(["-apply", "-env", TestUtil.env_test, "-repo", TestUtil.pgvctrl_test_repo, "-d",
                 TestUtil.pgvctrl_test_db], retcode=0)

        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{TestUtil.test_first_version}.0: ' \
                                       f'{TestUtil.pgvctrl_test_repo} environment (' \
                                       f'{TestUtil.env_test})\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_good_version_env(self):
        pgv = TestUtil.local_pgvctrl()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db,
                 "-setenv", TestUtil.env_test], retcode=0)
        arg_list = ["-apply", "-env", TestUtil.env_test, "-repo", TestUtil.pgvctrl_test_repo, "-d",
                    TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == f'Applied: {TestUtil.pgvctrl_test_repo} v {TestUtil.test_first_version}.0\n'


class TestPgvctrlTestDbError:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.get_error_sql()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_file(TestUtil.error_sql_path)
        TestUtil.delete_file(TestUtil.error_sql_rollback_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.drop_database()

    def test_apply_version_error_no_rollback(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 1
        assert 'AN ERROR WAS THROWN IN SQL' in rtn[TestUtil.stdout]
        assert 'Attempting to rollback' in rtn[TestUtil.stdout]
        assert '130.Error_rollback.sql: No such file or directory' in rtn[TestUtil.stdout]

    def test_apply_version_error_good_rollback(self):
        TestUtil.get_error_rollback_good_sql()
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert 'AN ERROR WAS THROWN IN SQL' in rtn[TestUtil.stdout]
        assert 'Attempting to rollback' in rtn[TestUtil.stdout]
        assert '130.Error_rollback' in rtn[TestUtil.stdout]
        assert '140.ItemsAddMore' in rtn[TestUtil.stdout]

    def test_apply_version_error_bad_rollback(self):
        TestUtil.get_error_rollback_bad_sql()
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 1
        assert 'AN ERROR WAS THROWN IN SQL' in rtn[TestUtil.stdout]
        assert 'Attempting to rollback' in rtn[TestUtil.stdout]
        assert '130.Error_rollback' in rtn[TestUtil.stdout]
        assert 'AN ERROR WAS THROWN IN THE ROLLBACK SQL' in rtn[TestUtil.stdout]
        assert '140.ItemsAddMore' not in rtn[TestUtil.stdout]
