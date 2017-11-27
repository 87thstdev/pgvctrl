import pytest
import sys
import dbversioning
from plumbum import colors, local, ProcessExecutionError
from test_util import TestUtil, print_cmd_error_details
# from dbversioning.dbvctrl import show_version

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

# TODO: Make tests for the following
# -mkconf
# -mkv 2.0.new_version -repo test_db
