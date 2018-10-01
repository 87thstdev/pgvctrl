import os
import io
import sys
from contextlib import redirect_stdout

from plumbum import local

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from dbversioning.dbvctrl import parse_args
from dbversioning.repositoryconf import (
    RepositoryConf,
    INCLUDE_TABLES,
    EXCLUDE_TABLES,
)
from test.test_util import (
    TestUtil,
    print_cmd_error_details,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


def test_no_args():
    arg_list = []
    dbvctrl_assert_simple_msg(
            arg_list=arg_list,
            msg=f"pgvctrl: No operation specified\nTry \"{Const.PGVCTRL} --help\" for more information.\n",
            error_code=1
    )


def test_version_main():
    pgv = local[Const.PGVCTRL]
    arg_list = [Const.VERSION_ARG]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout][:8] == "pgvctrl:"


def test_version():
    arg_list = [Const.VERSION_ARG]
    out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

    print_cmd_error_details(out_rtn, arg_list)
    assert errors is None
    assert out_rtn[:8] == "pgvctrl:"


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
    assert "Postgres db version control." in out_rtn


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
    assert "Postgres db version control." in out_rtn


class TestPgvctrlBadConf:
    def setup_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.get_static_bad_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_conf_bad_mkconf(self):
        arg_list = [Const.LIST_REPOS_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Bad config file!\n",
                error_code=1
        )


class TestPgvctrlNoConf:
    def setup_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_conf_no_mkconf(self):
        arg_list = [Const.LIST_REPOS_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File missing: {TestUtil.config_file}\n",
                error_code=1
        )


class TestPgvctrlRepoMakeConf:
    def setup_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_mkconf_not_exists(self):
        TestUtil.delete_file(TestUtil.config_file)

        arg_list = [Const.MKCONF_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Config file created: {TestUtil.config_file}\n"
        )

    def test_mkconf_exists(self):
        arg_list = [Const.MKCONF_ARG]

        # Make config first
        capture_dbvctrl_out(arg_list=arg_list)

        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File already exists: {TestUtil.config_file}\n",
                error_code=1
        )


class TestPgvctrlRepoMakeEnv:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_mkenv(self):
        arg_list = [
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Repository environment created: {TestUtil.pgvctrl_test_repo} {TestUtil.env_qa}\n"
        )


class TestPgvctrlSetRepoEnv:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_set_repo_env(self):
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SET_ENV_ARG,
                    TestUtil.env_qa,
                    Const.V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository environment set: {TestUtil.pgvctrl_test_repo} "
                    f"{TestUtil.env_qa} {TestUtil.test_version}\n"
        )

    def test_set_repo_env_no_v_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SET_ENV_ARG,
                    TestUtil.env_qa,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Missing {Const.V_ARG}\n",
                error_code=1
        )


class TestPgvctrlRepoMakeRemove:
    def setup_method(self):
        TestUtil.get_static_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_no_files_repo)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_mkrepo_not_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
                msg=f"Repository created: {TestUtil.pgvctrl_test_temp_repo}\n"
        )

    def test_mkrepo_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_no_files_repo],
                msg=f"Repository already exist: {TestUtil.pgvctrl_no_files_repo}\n",
                error_code=1
        )

    def test_mkrepo_bad_name(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.invalid_repo_name],
                msg=f"Repository name invalid {TestUtil.invalid_repo_name}\n",
                error_code=1
        )

    def test_rmrepo_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_no_files_repo],
                msg=f"Repository removed: {TestUtil.pgvctrl_no_files_repo}\n"
        )

    def test_rmrepo_not_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
                msg=f"Repository does not exist: {TestUtil.pgvctrl_test_temp_repo}\n",
                error_code=1
        )


class TestPgvctrlRepoList:
    def setup_method(self):
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.test_make_version_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_repo_list(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"\tv {TestUtil.test_first_version} test\n"
                    f"\tv {TestUtil.test_version} \n"
        )

    def test_repo_list_verbose(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_VERBOSE_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"\tv {TestUtil.test_first_version} test\n"
                    f"\tv {TestUtil.test_version} \n"
                    f"\t\t100 AddUsersTable\n"
                    f"\t\t110 Notice\n"
                    f"\t\t120 ItemTable\n"
                    f"\t\t140 ItemsAddMore\n"
                    f"\t\t200 AddEmailTable\n"
                    f"\t\t300 UserStateTable\n"
                    f"\t\t400 ErrorSet\n"
        )

    def test_repo_list_unregistered(self):
        capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])
        capture_dbvctrl_out(arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])
        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_temp_repo} UNREGISTERED\n"
                f"{TestUtil.pgvctrl_test_repo}\n"
                f"\tv {TestUtil.test_first_version} test\n"
                f"\tv {TestUtil.test_version} \n"
        )

    def test_repo_list_missing_repo(self):
        capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo])

        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                f"\tv {TestUtil.test_first_version} test\n"
                f"\tv {TestUtil.test_version} \n"
        )


class TestPgvctrlRepoRemoveEnv:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_rmenv_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.REMOVE_ENV_ARG,
                    TestUtil.env_qa,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo],
                msg=f"Invalid key in config, expected '{TestUtil.env_qa}'\n",
                error_code=1
        )

    def test_rmenv(self):
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.REMOVE_ENV_ARG,
                    TestUtil.env_qa,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository environment removed: {TestUtil.pgvctrl_test_repo} {TestUtil.env_qa}\n"
        )


class TestPgvctrlMakeVersion:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.test_make_version_path)

    def test_mkv(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Version {TestUtil.pgvctrl_test_repo}/{TestUtil.test_make_version} created.\n"
        )

    def test_mkv_bad(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_bad_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository version number invalid, should be [Major].[Minor].[Maintenance] "
                    f"at a minimum: {TestUtil.test_bad_version}\n",
                error_code=1
        )

    def test_mkv_exists(self):
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_V_ARG,
            TestUtil.test_make_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository version already exists: {TestUtil.pgvctrl_test_repo} 3.0.0\n",
                error_code=1
        )


class TestPgvctrlRepoIncludeSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_include_schema(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-schemas ['{TestUtil.schema_membership}']\n"
        )
        inc_sch_name = RepositoryConf.get_repo_include_schemas(
                TestUtil.pgvctrl_test_repo
        )
        assert inc_sch_name == ["membership"]


class TestPgvctrlRepoRemoveIncludeSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_include_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMINCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"include-schemas ['{TestUtil.schema_membership}']\n"
        )
        inc_sch_name = RepositoryConf.get_repo_include_schemas(
            TestUtil.pgvctrl_test_repo
        )
        assert inc_sch_name == []


class TestPgvctrlRepoExcludeSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_schema(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-schemas ['{TestUtil.schema_membership}']\n"
        )
        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(
            TestUtil.pgvctrl_test_repo
        )
        assert exc_sch_name == ["membership"]


class TestPgvctrlRepoRemoveExcludeSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_remove_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMEXCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-schemas ['{TestUtil.schema_membership}']\n"
        )

        sch_name = RepositoryConf.get_repo_exclude_schemas(
            TestUtil.pgvctrl_test_repo
        )
        assert sch_name == []


class TestPgvctrlRepoExcludeIncludedSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_schema(self):
        # Exclude first
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-schemas ['{TestUtil.schema_membership}']\n"
        )

        inc_sch_name = RepositoryConf.get_repo_include_schemas(
            TestUtil.pgvctrl_test_repo
        )
        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(
            TestUtil.pgvctrl_test_repo
        )

        assert inc_sch_name == ["membership"]
        assert exc_sch_name == []


class TestPgvctrlRepoIncludeExcludedSchema:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_schema(self):
        # Include first
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_SCHEMA_ARG,
                    TestUtil.schema_membership,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-schemas ['{TestUtil.schema_membership}']\n"
        )

        inc_sch_name = RepositoryConf.get_repo_include_schemas(
            TestUtil.pgvctrl_test_repo
        )
        exc_sch_name = RepositoryConf.get_repo_exclude_schemas(
            TestUtil.pgvctrl_test_repo
        )

        assert inc_sch_name == []
        assert exc_sch_name == ["membership"]


class TestPgvctrlRepoIncludeTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_include_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.INCLUDE_TABLE_ARG,
                    TestUtil.table_public_item,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}', '{TestUtil.table_public_item}']\n"
        )

        name_list = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES
        )

        assert set(name_list) == {
            TestUtil.table_public_item,
            TestUtil.table_membership_user_state,
        }


class TestPgvctrlRepoRemoveIncludeTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_remove_include_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMINCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_table_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES
        )
        assert inc_table_name == []


class TestPgvctrlRepoExcludeTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        name_list = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES
        )
        assert name_list == [TestUtil.table_membership_user_state]


class TestPgvctrlRepoRemoveExcludeTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_remove_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMEXCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        exc_table_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES
        )
        assert exc_table_name == []


class TestPgvctrlRepoIncludeExcludedTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_schema(self):
        # Include first
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES
        )
        exc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES
        )
        assert inc_tbl_name == []
        assert exc_tbl_name == [TestUtil.table_membership_user_state]


class TestPgvctrlRepoExcludedIncludeTable:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_exclude_schema(self):
        # Exclude first
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_TABLE_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES
        )

        exc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES
        )
        assert inc_tbl_name == [TestUtil.table_membership_user_state]
        assert exc_tbl_name == []
