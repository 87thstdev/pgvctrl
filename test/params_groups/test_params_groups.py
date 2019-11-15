from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)
import dbversioning.dbvctrlConst as Const

SOMETHING = 'some_name'


def test_status_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.STATUS,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db
            ],
            msg='',
            error_code=2
    )


def test_chkver_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.CHECK_VER_ARG,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db
            ],
            msg='',
            error_code=2
    )


def test_init_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.INIT_ARG,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db
            ],
            msg='',
            error_code=2
    )


def test_mkv_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.MAKE_V_ARG,
                TestUtil.test_make_version
            ],
            msg='',
            error_code=2
    )


def test_rmv_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.REMOVE_V_ARG,
                TestUtil.test_make_version
            ],
            msg='',
            error_code=2
    )


def test_mkenv_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.MAKE_ENV_ARG,
                TestUtil.env_qa],
            msg='',
            error_code=2
    )


def test_rmenv_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.REMOVE_ENV_ARG,
                TestUtil.env_qa],
            msg='',
            error_code=2
    )


def test_set_version_storage_owner_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_set_env_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.SET_ENV_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_include_schema_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.INCLUDE_SCHEMA_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_include_schema_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.INCLUDE_SCHEMA_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rm_schema_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMINCLUDE_SCHEMA_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rm_schema_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMINCLUDE_SCHEMA_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_exclude_schema_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.EXCLUDE_SCHEMA_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_exclude_schema_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.EXCLUDE_SCHEMA_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rmexclude_schema_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMEXCLUDE_SCHEMA_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rmexclude_schema_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMEXCLUDE_SCHEMA_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_include_table_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.INCLUDE_TABLE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_include_table_fail_long_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.INCLUDE_TABLE_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rminclude_table_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMINCLUDE_TABLE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rminclude_table_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMINCLUDE_TABLE_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_exclude_table_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.EXCLUDE_TABLE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_exclude_table_long_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.EXCLUDE_TABLE_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rmexclude_table_fail_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMEXCLUDE_TABLE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_rmexclude_table_fail_long_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RMEXCLUDE_TABLE_LONG_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_apply_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.APPLY_ARG,
                Const.V_ARG,
                SOMETHING,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_getss_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.GETSS_ARG,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_applyss_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.APPLY_SS_ARG,
                SOMETHING,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_applyss_long_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.APPLY_SS_LONG_ARG,
                SOMETHING,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_pulldata_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.PULL_DATA_ARG,
                Const.DATA_TBL_ARG,
                SOMETHING,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_pushdata_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.PUSH_DATA_ARG,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_dump_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.DUMP_ARG,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )


def test_restore_no_repo():
    dbvctrl_assert_simple_msg(
            arg_list=[
                Const.RESTORE_ARG,
                SOMETHING,
                Const.DATABASE_ARG,
                SOMETHING],
            msg='',
            error_code=2
    )
