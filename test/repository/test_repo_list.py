import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from dbversioning.repositoryconf import (
    INCLUDE_TABLES_PROP,
    EXCLUDE_TABLES_PROP,
    INCLUDE_SCHEMAS_PROP,
    EXCLUDE_SCHEMAS_PROP)
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestRepoList:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.create_root_folder()
        TestUtil.delete_folder_full(TestUtil.database_folder)

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_repo_list_no_root(self):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.LIST_REPOS_VERBOSE_ARG])

        assert errors is None
        assert out_rtn == "No Repositories available.\n"


class TestRepoListWith:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.create_root_folder()
        TestUtil.mkrepo(repo_name=TestUtil.pgvctrl_test_repo)
        TestUtil.mkrepo_ver(
                repo_name=TestUtil.pgvctrl_test_repo, version=TestUtil.test_version
        )
        TestUtil.mkrepo_ver(
            repo_name=TestUtil.pgvctrl_test_repo, version=TestUtil.test_first_version
        )
        TestUtil.mkrepo_ver(
                repo_name=TestUtil.pgvctrl_test_repo, version=TestUtil.test_second_version_no_name
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="100.sql"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="200.Error.sql"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="200.Error_rollback.sql"
        )
        capture_dbvctrl_out(
                arg_list=[
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.MAKE_ENV_ARG,
                    TestUtil.env_test
                ]
        )
        capture_dbvctrl_out(
                arg_list=[
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.V_ARG,
                    TestUtil.test_first_version,
                    Const.SET_ENV_ARG,
                    TestUtil.env_test
                ]
        )


    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_repo_list(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
                    f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
                    f"{Const.TAB}v {TestUtil.test_version} \n"
        )

    def test_repo_list_verbose(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
                    f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
                    f"{Const.TAB}v {TestUtil.test_version} \n"
                    f"{Const.TABS}100 \n"
                    f"{Const.TABS}200 Error\n"
                    f"{Const.TABS}200 Error_rollback ROLLBACK\n"
        )

    def test_repo_list_verbose_includes_excludes(self):
        base_msg = (
            f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
            f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
            f"{Const.TAB}v {TestUtil.test_version} \n"
            f"{Const.TABS}100 \n"
            f"{Const.TABS}200 Error\n"
            f"{Const.TABS}200 Error_rollback ROLLBACK\n"
        )

        capture_dbvctrl_out(
                arg_list=[
                    Const.INCLUDE_SCHEMA_LONG_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}({INCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_membership}'])\n"
                    f"{base_msg}"
        )

        capture_dbvctrl_out(
                arg_list=[
                    Const.INCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}({INCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_membership}'], "
                    f"{INCLUDE_TABLES_PROP}: ['{TestUtil.table_membership_user_state}'])\n"
                    f"{base_msg}"
        )

        capture_dbvctrl_out(
                arg_list=[
                    Const.EXCLUDE_SCHEMA_LONG_ARG,
                    TestUtil.schema_public,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}({INCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_membership}'], "
                    f"{EXCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_public}'], "
                    f"{INCLUDE_TABLES_PROP}: ['{TestUtil.table_membership_user_state}'])\n"
                    f"{base_msg}"
        )

        capture_dbvctrl_out(
                arg_list=[
                    Const.EXCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_public_item,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}({INCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_membership}'], "
                    f"{EXCLUDE_SCHEMAS_PROP}: ['{TestUtil.schema_public}'], "
                    f"{INCLUDE_TABLES_PROP}: ['{TestUtil.table_membership_user_state}'], "
                    f"{EXCLUDE_TABLES_PROP}: ['{TestUtil.table_public_item}'])\n"
                    f"{base_msg}"
        )

    def test_repo_list_unregistered(self):
        capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])
        capture_dbvctrl_out(arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_temp_repo} UNREGISTERED\n"
                f"{TestUtil.pgvctrl_test_repo}\n"
                f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
                f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
                f"{Const.TAB}v {TestUtil.test_version} \n"
        )

    def test_repo_list_missing_repo(self):
        capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])

        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
                f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
                f"{Const.TAB}v {TestUtil.test_version} \n"
        )

    def test_repo_list_bad_sql_name(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version="1.0.0",
                file_name=TestUtil.bad_sql_name
        )

        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.LIST_REPOS_VERBOSE_ARG])

        assert errors.code == 1
        assert out_rtn.startswith("Sql filename error:")
        assert TestUtil.bad_sql_name in out_rtn

        TestUtil.delete_file(f"{TestUtil.test_sql_path}/{TestUtil.bad_sql_name}")


class TestBadRepoList:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.mkrepo(repo_name=TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_make_bad_repo_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.REPO_ARG,
                          TestUtil.pgvctrl_test_temp_repo,
                          Const.MAKE_V_ARG,
                          TestUtil.test_bad_version_number],
                msg=f"Repository version number invalid, should be [Major].[Minor].[Maintenance] "
                    f"at a minimum: {TestUtil.test_bad_version_number}\n",
                error_code=1
        )
