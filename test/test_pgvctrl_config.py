import os
import io
import sys
from contextlib import redirect_stdout

from dbversioning.dbvctrlConst import (
    RMINCLUDE_SCHEMA_ARG,
    RMEXCLUDE_SCHEMA_ARG,
    INCLUDE_TABLE_ARG,
    EXCLUDE_TABLE_ARG,
    RMINCLUDE_TABLE_ARG,
    RMEXCLUDE_TABLE_ARG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dbversioning.dbvctrl import (
    parse_args,
    INCLUDE_SCHEMA_ARG,
    EXCLUDE_SCHEMA_ARG)
from dbversioning.repositoryconf import (
    RepositoryConf,
    INCLUDE_TABLES,
    EXCLUDE_TABLES)
from test.test_util import (
    TestUtil,
    print_cmd_error_details,
    capture_dbvctrl_out)

DB_REPO_CONFIG_JSON = 'dbRepoConfig.json'


def test_version():
    arg_list = ['-version']
    args = parse_args(arg_list)
    out_rtn, errors = capture_dbvctrl_out(args=args)

    print_cmd_error_details(out_rtn, arg_list)
    assert errors is None
    assert out_rtn[:8] == 'pgvctrl:'


def test_help_h():
    arg_list = ["-h"]
    out = io.StringIO()
    errors = None

    with redirect_stdout(out):
        try:
            parse_args(arg_list)
        except BaseException as e:
            errors = e

    out_rtn = out.getvalue()

    print_cmd_error_details(out_rtn, arg_list)
    assert errors.code == 0
    assert 'Postgres db version control.' in out_rtn


def test_help_help():
    arg_list = ["--help"]
    out = io.StringIO()
    errors = None

    with redirect_stdout(out):
        try:
            parse_args(arg_list)
        except BaseException as e:
            errors = e

    out_rtn = out.getvalue()

    print_cmd_error_details(out_rtn, arg_list)
    assert errors.code == 0
    assert 'Postgres db version control.' in out_rtn


class TestPgvctrlRepoMakeConf:
    def setup_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_mkconf_not_exists(self):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

        arg_list = ['-mkconf']
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == 'Config file created: {0}\n'.format(DB_REPO_CONFIG_JSON)
        assert errors is None

    def test_mkconf_exists(self):
        arg_list = ["-mkconf"]
        args = parse_args(arg_list)

        # Make config first
        capture_dbvctrl_out(args=args)

        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'File already exists: {DB_REPO_CONFIG_JSON}\n'
        assert errors.code == 1


class TestPgvctrlRepoMakeEnv:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_mkenv(self):
        arg_list = [
            "-mkenv", TestUtil.env_qa,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f"Repository environment created: {TestUtil.pgvctrl_test_repo} {TestUtil.env_qa}\n"
        assert errors is None


class TestPgvctrlRepoMakeRemove:
    def setup_method(self, test_method):
        TestUtil.get_static_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_no_files_repo)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_mkrepo_not_exists(self):
        arg_list = ["-mkrepo", TestUtil.pgvctrl_test_temp_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository created: {TestUtil.pgvctrl_test_temp_repo}\n'
        assert errors is None

    def test_mkrepo_exists(self):
        arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository already exist: {TestUtil.pgvctrl_no_files_repo}\n'
        assert errors.code == 1

    def test_rmrepo_exists(self):
        arg_list = ["-rmrepo", TestUtil.pgvctrl_no_files_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository removed: {TestUtil.pgvctrl_no_files_repo}\n'
        assert errors is None

    def test_rmrepo_not_exists(self):
        arg_list = ["-rmrepo", TestUtil.pgvctrl_test_temp_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository does not exist: {TestUtil.pgvctrl_test_temp_repo}\n'
        assert errors.code == 1


class TestPgvctrlRepoList:
    def setup_method(self, test_method):
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.test_make_version_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_repo_list(self):
        arg_list = ["-rl"]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'{TestUtil.pgvctrl_test_repo}\n' \
                          f'\tv {TestUtil.test_first_version} test\n' \
                          f'\tv {TestUtil.test_version} \n'
        assert errors is None

    def test_repo_list_verbose(self):
        arg_list = ["-rlv"]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'{TestUtil.pgvctrl_test_repo}\n' \
                          f'\tv {TestUtil.test_first_version} test\n' \
                          f'\tv {TestUtil.test_version} \n' \
                          f'\t\t100 AddUsersTable\n' \
                          f'\t\t110 Notice\n' \
                          f'\t\t120 ItemTable\n' \
                          f'\t\t140 ItemsAddMore\n' \
                          f'\t\t200 AddEmailTable\n' \
                          f'\t\t300 UserStateTable\n' \
                          f'\t\t400 ErrorSet\n'
        assert errors is None

    def test_repo_list_unregistered(self):
        arg_list = ["-mkrepo", TestUtil.pgvctrl_test_temp_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-rmrepo", TestUtil.pgvctrl_test_temp_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-rl"]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'{TestUtil.pgvctrl_test_temp_repo} UNREGISTERED\n' \
                          f'{TestUtil.pgvctrl_test_repo}\n' \
                          f'\tv {TestUtil.test_first_version} test\n' \
                          f'\tv {TestUtil.test_version} \n'
        assert errors is None


class TestPgvctrlMakeVersion:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.test_make_version_path)

    def test_mkv(self):
        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Version {TestUtil.pgvctrl_test_repo}/{TestUtil.test_make_version} created.\n'
        assert errors is None

    def test_mkv_bad(self):
        arg_list = ["-mkv", TestUtil.test_bad_version, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository version number invalid, should be [Major].[Minor].[Maintenance] ' \
                          f'at a minimum: {TestUtil.test_bad_version}\n'
        assert errors.code == 1

    def test_mkv_exists(self):
        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        print_cmd_error_details(out_rtn, arg_list)
        assert out_rtn == f'Repository version already exists: {TestUtil.pgvctrl_test_repo} 3.0.0\n'
        assert errors.code == 1


class TestPgvctrlRepoIncludeSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_include_schema(self):
        arg_list = [
            INCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_sch_name = RepositoryConf.get_repo_include_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_sch_name == ['membership']
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"include-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoRemoveIncludeSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_include_schema(self):
        arg_list = [
            INCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [
            RMINCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_sch_name = RepositoryConf.get_repo_include_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_sch_name == []
        assert out_rtn == f"Repository removed: {TestUtil.pgvctrl_test_repo} " \
                          f"include-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoExcludeSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_schema(self):
        arg_list = [
            EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert exc_sch_name == ['membership']
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoRemoveExcludeSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_remove_exclude_schema(self):
        ex_arg_list = [
            EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(ex_arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [
            RMEXCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        sch_name = RepositoryConf.get_repo_exclude_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert sch_name == []
        assert out_rtn == f"Repository removed: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoExcludeIncludedSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_schema(self):
        arg_list = [
            EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Exclude first
        capture_dbvctrl_out(args=args)

        arg_list = [
            INCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Include second
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_sch_name = RepositoryConf.get_repo_include_schemas(TestUtil.pgvctrl_test_repo)
        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_sch_name == ['membership']
        assert exc_sch_name == []
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"include-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoIncludeExcludedSchema:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_schema(self):
        arg_list = [
            INCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Include first
        capture_dbvctrl_out(args=args)

        arg_list = [
            EXCLUDE_SCHEMA_ARG, TestUtil.schema_membership,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Exclude second
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_sch_name = RepositoryConf.get_repo_include_schemas(TestUtil.pgvctrl_test_repo)
        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(TestUtil.pgvctrl_test_repo)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_sch_name == []
        assert exc_sch_name == ['membership']
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-schemas ['{TestUtil.schema_membership}']\n"


class TestPgvctrlRepoIncludeTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_include_table(self):
        arg_list = [
            INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            INCLUDE_TABLE_ARG, TestUtil.table_public_item,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        name_list = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=INCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert set(name_list) == {TestUtil.table_public_item, TestUtil.table_membership_user_state}
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"include-table ['{TestUtil.table_membership_user_state}', '{TestUtil.table_public_item}']\n"


class TestPgvctrlRepoRemoveIncludeTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_remove_include_schema(self):
        arg_list = [
            INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [
            RMINCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_table_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=INCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_table_name == []
        assert out_rtn == f"Repository removed: {TestUtil.pgvctrl_test_repo} " \
                          f"include-table ['{TestUtil.table_membership_user_state}']\n"


class TestPgvctrlRepoExcludeTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_table(self):
        arg_list = [
            EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        name_list = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=EXCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert name_list == [TestUtil.table_membership_user_state]
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-table ['{TestUtil.table_membership_user_state}']\n"


class TestPgvctrlRepoRemoveExcludeTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_remove_exclude_schema(self):
        arg_list = [
            EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        capture_dbvctrl_out(args=args)

        arg_list = [
            RMEXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        out_rtn, errors = capture_dbvctrl_out(args=args)

        exc_table_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=EXCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert exc_table_name == []
        assert out_rtn == f"Repository removed: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-table ['{TestUtil.table_membership_user_state}']\n"


class TestPgvctrlRepoIncludeExcludedTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_schema(self):
        arg_list = [
            INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Include first
        capture_dbvctrl_out(args=args)

        arg_list = [
            EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Exclude second
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_tbl_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=INCLUDE_TABLES)

        exc_tbl_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=EXCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_tbl_name == []
        assert exc_tbl_name == [TestUtil.table_membership_user_state]
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"exclude-table ['{TestUtil.table_membership_user_state}']\n"


class TestPgvctrlRepoExcludedIncludeTable:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_exclude_schema(self):
        arg_list = [
            EXCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Exclude first
        capture_dbvctrl_out(args=args)

        arg_list = [
            INCLUDE_TABLE_ARG, TestUtil.table_membership_user_state,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        args = parse_args(arg_list)
        # Include second
        out_rtn, errors = capture_dbvctrl_out(args=args)

        inc_tbl_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=INCLUDE_TABLES)

        exc_tbl_name = RepositoryConf.get_repo_list(
                repo_name=TestUtil.pgvctrl_test_repo,
                list_name=EXCLUDE_TABLES)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert inc_tbl_name == [TestUtil.table_membership_user_state]
        assert exc_tbl_name == []
        assert out_rtn == f"Repository added: {TestUtil.pgvctrl_test_repo} " \
                          f"include-table ['{TestUtil.table_membership_user_state}']\n"
