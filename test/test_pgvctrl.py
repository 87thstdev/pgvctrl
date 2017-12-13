import pytest
import sys
import dbversioning
from plumbum import colors, local, ProcessExecutionError
from test_util import TestUtil, print_cmd_error_details

DB_REPO_CONFIG_JSON = 'dbRepoConfig.json'

def test_version():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-version"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0
    assert rtn[TestUtil.stdout][:8] == 'pgvctrl:'


def test_help_h():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-h"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_help_help():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["--help"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_repo_list():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-repolist"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_repo_list_verbose():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-repolist", "-verbose"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0

def test_mkconf_not_exists():
    TestUtil.delete_file(DB_REPO_CONFIG_JSON)
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == 'Config file created: {0}\n'.format(DB_REPO_CONFIG_JSON)
    assert rtn[TestUtil.return_code] == 0

def test_mkconf_exists():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    rtn = pgv.run(arg_list, retcode=0)

    arg_list = ["-mkconf"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == 'File already exists: {0}\n'.format(DB_REPO_CONFIG_JSON)
    assert rtn[TestUtil.return_code] == 0
