import os
import io
import sys
from contextlib import redirect_stdout

from plumbum import local

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from dbversioning.dbvctrl import parse_args
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


def test_mkrepo_not_exists():
    dbvctrl_assert_simple_msg(
            arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
            msg=f"File missing: dbRepoConfig.json\n",
            error_code=1
    )


def test_rmrepo_not_exists():
    dbvctrl_assert_simple_msg(
            arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
            msg=f"File missing: dbRepoConfig.json\n",
            error_code=1
    )
