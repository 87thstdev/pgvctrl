import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from dbversioning.repositoryconf import (
    RepositoryConf,
    INCLUDE_TABLES_PROP,
    EXCLUDE_TABLES_PROP)
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestConfigUpdating:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_set_repo_table_owner(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
                    TestUtil.version_table_owner,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository version storage owner set: "
                    f"{TestUtil.pgvctrl_test_repo} {TestUtil.version_table_owner}\n"
        )

    def test_set_repo_table_owner_bad_repo(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
                    TestUtil.version_table_owner,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_bad_repo,
                ],
                msg=f"Repository does not exist: {TestUtil.pgvctrl_bad_repo}\n",
                error_code=1
        )

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

    def test_mkenv_bad_repo(self):
        arg_list = [
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_bad_repo,
        ]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Repository does not exist: {TestUtil.pgvctrl_bad_repo}\n",
                error_code=1
        )

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

    def test_set_repo_two_on_ver_env(self):
        base_msg = (
            f"{TestUtil.pgvctrl_test_repo}\n"
            f"{Const.TAB}v {TestUtil.test_second_version_no_name} \n"
            f"{Const.TAB}v {TestUtil.test_version} ['{TestUtil.env_qa}', '{TestUtil.env_test}']\n"
        )

        capture_dbvctrl_out(arg_list=[
            Const.MAKE_ENV_ARG,
            TestUtil.env_qa,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(
                arg_list=[
                    Const.SET_ENV_ARG,
                    TestUtil.env_test,
                    Const.V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        capture_dbvctrl_out(
                arg_list=[
                    Const.SET_ENV_ARG,
                    TestUtil.env_qa,
                    Const.V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_REPOS_ARG
                ],
                msg=base_msg
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

    def test_include_schema(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_SCHEMA_LONG_ARG,
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

    def test_remove_include_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMINCLUDE_SCHEMA_LONG_ARG,
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

    def test_exclude_schema(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_SCHEMA_LONG_ARG,
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

    def test_remove_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMEXCLUDE_SCHEMA_LONG_ARG,
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

    def test_exclude_schema_then_include(self):
        # Exclude first
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_SCHEMA_LONG_ARG,
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

    def test_include_schema_then_exclude(self):
        # Include first
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_SCHEMA_LONG_ARG,
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

    def test_include_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.INCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_public_item,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}', '{TestUtil.table_public_item}']\n"
        )

        name_list = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES_PROP
        )

        assert set(name_list) == {
            TestUtil.table_public_item,
            TestUtil.table_membership_user_state,
        }

    def test_remove_include_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMINCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_table_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES_PROP
        )
        assert inc_table_name == []

    def test_exclude_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        name_list = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES_PROP
        )
        assert name_list == [TestUtil.table_membership_user_state]

    def test_remove_exclude_table(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.RMEXCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository removed: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        exc_table_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES_PROP
        )
        assert exc_table_name == []

    def test_include_table_then_exclude(self):
        # Include first
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.EXCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"exclude-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES_PROP
        )
        exc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES_PROP
        )
        assert inc_tbl_name == []
        assert exc_tbl_name == [TestUtil.table_membership_user_state]

    def test_exclude_table_then_include(self):
        # Exclude first
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_TABLE_LONG_ARG,
            TestUtil.table_membership_user_state,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INCLUDE_TABLE_LONG_ARG,
                    TestUtil.table_membership_user_state,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                ],
                msg=f"Repository added: {TestUtil.pgvctrl_test_repo} "
                    f"include-table ['{TestUtil.table_membership_user_state}']\n"
        )

        inc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=INCLUDE_TABLES_PROP
        )

        exc_tbl_name = RepositoryConf.get_repo_list(
            repo_name=TestUtil.pgvctrl_test_repo, list_name=EXCLUDE_TABLES_PROP
        )
        assert inc_tbl_name == [TestUtil.table_membership_user_state]
        assert exc_tbl_name == []


class TestConfigMakeRepo:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_mkrepo(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
                msg=f"Repository created: {TestUtil.pgvctrl_test_temp_repo}\n"
        )
        repo_json = (
            '        {\n'
            '            "dumpDatabaseOptions": "-Fc -Z4",\n'
            '            "envs": {},\n'
            f'            "name": "{TestUtil.pgvctrl_test_temp_repo}",\n'
            '            "restoreDatabaseOptions": "-Fc -j 8",\n'
            '            "versionStorage": {\n'
            '                "env": "env",\n'
            '                "isProduction": "is_production",\n'
            '                "repository": "repository_name",\n'
            '                "revision": "revision",\n'
            '                "table": "repository_version",\n'
            '                "tableOwner": null,\n'
            '                "version": "version",\n'
            '                "versionHash": "version_hash"\n'
            '            }\n'
            '        }'
        )
        has_repo = TestUtil.file_contains(TestUtil.config_file, repo_json)

        assert has_repo is True
