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
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_repo, TestUtil.test_second_version_no_name
        )
        TestUtil.get_error_sql()
        TestUtil.get_error_rollback_good_sql()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.test_make_version_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)
        TestUtil.delete_file(TestUtil.error_sql_path)
        TestUtil.delete_file(TestUtil.error_sql_rollback_path)

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
                    f"{Const.TABS}90 \n"
                    f"{Const.TABS}100 AddUsersTable\n"
                    f"{Const.TABS}110 Notice\n"
                    f"{Const.TABS}120 ItemTable\n"
                    f"{Const.TABS}130 Error\n"
                    f"{Const.TABS}130 Error_rollback ROLLBACK\n"
                    f"{Const.TABS}140 ItemsAddMore\n"
                    f"{Const.TABS}200 AddEmailTable\n"
                    f"{Const.TABS}300 UserStateTable\n"
                    f"{Const.TABS}400 ErrorSet\n"
        )

    def test_repo_list_verbose_includes_excludes(self):
        base_msg = (
            f"{Const.TAB}v {TestUtil.test_first_version} ['{TestUtil.env_test}']\n"
            f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
            f"{Const.TAB}v {TestUtil.test_version} \n"
            f"{Const.TABS}90 \n"
            f"{Const.TABS}100 AddUsersTable\n"
            f"{Const.TABS}110 Notice\n"
            f"{Const.TABS}120 ItemTable\n"
            f"{Const.TABS}130 Error\n"
            f"{Const.TABS}130 Error_rollback ROLLBACK\n"
            f"{Const.TABS}140 ItemsAddMore\n"
            f"{Const.TABS}200 AddEmailTable\n"
            f"{Const.TABS}300 UserStateTable\n"
            f"{Const.TABS}400 ErrorSet\n"
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
        TestUtil.get_static_bad_sql_name()

        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.LIST_REPOS_VERBOSE_ARG])

        assert errors.code == 1
        assert out_rtn.startswith("Sql filename error:")
        assert TestUtil.bad_sql_name in out_rtn

        TestUtil.delete_file(f"{TestUtil.test_sql_path}/{TestUtil.bad_sql_name}")


class TestBadRepoList:
    def setup_method(self):
        TestUtil.get_static_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

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
