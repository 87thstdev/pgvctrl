import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg,
    capture_dbvctrl_out)


class TestBadConf:
    def setup_method(self):
        pass

    def teardown_method(self):
        TestUtil.remove_config()

    def test_mkconf_not_exists(self):
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[Const.MKCONF_ARG]
        )
        assert errors is None
        assert out_rtn == f"Config file created: {TestUtil.config_file}\n"

    def test_mkconf_exists(self):
        arg_list = [Const.MKCONF_ARG]

        # Make config first
        capture_dbvctrl_out(arg_list=arg_list)

        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File already exists: {TestUtil.config_file}\n",
                error_code=1
        )

    def test_conf_bad_root(self):
        TestUtil.create_config("{}")

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"Invalid key in config, expected 'root'\n",
                error_code=1
        )

    def test_conf_no_mkconf(self):
        arg_list = [Const.LIST_REPOS_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File missing: {TestUtil.config_file}\n",
                error_code=1
        )


class TestBadConfRepos:
    def setup_method(self):
        TestUtil.create_repo_folder("pgvctrl_test")

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_conf_bad_repositories(self):
        TestUtil.create_config(contents=('''{
            "defaultVersionStorage": {
                "env": "env",
                "isProduction": "is_production",
                "repository": "repository_name",
                "revision": "revision",
                "table": "repository_version",
                "tableOwner": null,
                "version": "version",
                "versionHash": "version_hash"
            },
            "repositoriesBAD": [],
            "root": "databases"
        }'''))

        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[Const.LIST_REPOS_ARG],
        )

        assert out_rtn == f"Invalid key in config, expected 'repositories'\n"
        assert errors.code == 1

    def test_conf_bad_multi_repositories(self):
        TestUtil.create_config(contents='''{
            "defaultVersionStorage": {
                "env": "env",
                "isProduction": "is_production",
                "repository": "repository_name",
                "revision": "revision",
                "table": "repository_version",
                "version": "version",
                "versionHash": "version_hash",
                "tableOwner": null
            },
            "repositories": [
                {
                    "name": "pgvctrl_test",
                    "versionStorage": {
                        "env": "env",
                        "isProduction": "is_production",
                        "repository": "repository_name",
                        "revision": "revision",
                        "table": "repository_version",
                        "version": "version",
                        "versionHash": "version_hash",
                        "tableOwner": null
                    }
                },
                {
                    "name": "pgvctrl_test",
                    "versionStorage": {
                        "env": "env",
                        "isProduction": "is_production",
                        "repository": "repository_name",
                        "revision": "revision",
                        "table": "repository_version",
                        "version": "version",
                        "versionHash": "version_hash",
                        "tableOwner": null
                    }
                }
             ],
            "root": "databases"
        }''')

        TestUtil.create_repo_folder("pgvctrl_test")
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[Const.LIST_REPOS_ARG],
        )
        assert out_rtn == f"Bad config: Multiple repositories found for {TestUtil.pgvctrl_test_repo}!\n"
        assert errors.code == 1
