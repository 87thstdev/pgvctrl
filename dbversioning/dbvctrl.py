import argparse
import sys

from plumbum import ProcessExecutionError
import pkg_resources
import dbversioning.dbvctrlConst as Const
from dbversioning.errorUtil import (
    VersionedDbException,
    VersionedDbExceptionProductionChangeNoProductionFlag)
from dbversioning.versionedDbConnection import connection_list
from dbversioning.versionedDbShellUtil import (
    information_message,
    error_message)
from dbversioning.versionedDbUtil import VersionedDbHelper


def parse_args(args):
    parser = argparse.ArgumentParser(description='Postgres db version control.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument(Const.VERSION_ARG, help='Show dbvctrl version number', action='store_true')
    group.add_argument(Const.INIT_ARG, help='Initialize database on server for version control', action='store_true')
    group.add_argument(Const.LIST_REPOS_ARG, help='List repositories', action='store_true')
    group.add_argument(Const.LIST_REPOS_VERBOSE_ARG, help='List repositories verbose', action='store_true')
    group.add_argument(Const.CHECK_VER_ARG, help='Check database version', action='store_true')
    group.add_argument(Const.MKCONF_ARG, help='Create dbRepoConfig.json', action='store_true')
    group.add_argument(Const.APPLY_ARG, help='Apply sql version', action='store_true')
    group.add_argument(Const.SETFF_ARG, help='Set version fast forward', action='store_true')
    group.add_argument(Const.APPLY_FF_ARG, help='Apply version fast forward')
    group.add_argument(Const.PULL_DATA_ARG, help='Pull data from repository by table', action='store_true')
    group.add_argument(Const.PUSH_DATA_ARG, help='Push data from repository to database', action='store_true')

    parser.add_argument(Const.FORCE_ARG, help='Force push data from repository to database', action='store_true')

    parser.add_argument(Const.TBL_ARG, metavar='', help='Pull table for data', action='append')

    parser.add_argument(Const.V_ARG, metavar='', help='Version number')
    parser.add_argument(Const.MAKE_REPO_ARG, metavar='', help='Make Repository')
    parser.add_argument(Const.REMOVE_REPO_ARG, metavar='', help='Remove Repository')
    parser.add_argument(Const.MAKE_V_ARG, metavar='', help='Make version number')
    parser.add_argument(Const.MAKE_ENV_ARG, metavar='', help='Make environment type')
    parser.add_argument(Const.REMOVE_ENV_ARG, metavar='', help='Remove environment type')
    parser.add_argument(Const.SET_ENV_ARG, metavar='', help='Set environment type to a version')
    parser.add_argument(Const.ENV_ARG, metavar='', help='Repository environment name')
    parser.add_argument(Const.REPO_ARG, metavar='', help='Repository name')
    parser.add_argument(Const.PRODUCTION_ARG, help='Database production flag', action='store_true')

    parser.add_argument(Const.SERVICE_ARG, metavar='', help='pg service')

    parser.add_argument(Const.DATABASE_ARG, metavar='', help='database name on server')
    parser.add_argument(Const.HOST_ARG, metavar='', help='postgres server host')
    parser.add_argument(Const.PORT_ARG, metavar='', help='port')
    parser.add_argument(Const.USER_ARG, metavar='', help='database username')
    parser.add_argument(Const.PWD_ARG, metavar='', help='password')

    parser.add_argument(Const.INCLUDE_SCHEMA_ARG, '-is', metavar='', help='Add schema to include schema list', action='append')
    parser.add_argument(Const.INCLUDE_TABLE_ARG, '-it', metavar='', help='Add table to include table list', action='append')
    parser.add_argument(Const.EXCLUDE_SCHEMA_ARG, '-es', metavar='', help='Add schema to exclude schema list', action='append')
    parser.add_argument(Const.EXCLUDE_TABLE_ARG, '-et', metavar='', help='Add table to exclude table list', action='append')
    parser.add_argument(Const.RMINCLUDE_SCHEMA_ARG, '-rmis', metavar='', help='Remove schema from include schema list',
                        action='append')
    parser.add_argument(Const.RMINCLUDE_TABLE_ARG, '-rmit', metavar='', help='Remove table from include table list',
                        action='append')
    parser.add_argument(Const.RMEXCLUDE_SCHEMA_ARG, '-rmes', metavar='', help='Remove schema from exclude schema list',
                        action='append')
    parser.add_argument(Const.RMEXCLUDE_TABLE_ARG, '-rmet', metavar='', help='Remove table from exclude table list',
                        action='append')

    return parser.parse_args(args)


def display_repo_list(verbose=False):
    c = VersionedDbHelper()
    c.display_repo_list(verbose)


def check_db_version_on_server(arg_set):
    c = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    c.display_db_version_on_server(db_conn, arg_set.repo)


def initialize_versioned_db(arg_set):
    init = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    init.initialize_db_version_on_server(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            is_production=arg_set.production,
            env=arg_set.setenv
    )


def apply_repository_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)
    version = _get_repo_version(arg_set)

    vdb.apply_repository_to_database(
        db_conn=db_conn,
        repo_name=arg_set.repo,
        version=version,
        is_production=arg_set.production,
        env=arg_set.env
    )


def _get_repo_version(arg_set):
    if arg_set.v and arg_set.env:
        raise VersionedDbException("Version and environment args are mutually exclusive.")

    if arg_set.v:
        return arg_set.v

    vdb = VersionedDbHelper()
    return vdb.get_repository_environment(
            repo_name=arg_set.repo,
            env=arg_set.env
    )


def apply_fast_forward_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.apply_repository_fast_forward_to_database(
        db_conn=db_conn,
        repo_name=arg_set.repo,
        full_version=arg_set.applyff
    )


def push_repo_data_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.push_data_to_database(
        db_conn=db_conn,
        repo_name=arg_set.repo,
        force=arg_set.force,
        is_production=arg_set.production
    )


def pull_table_for_repo_data(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.pull_table_for_repo_data(
        db_conn=db_conn,
        repo_name=arg_set.repo,
        table_list=arg_set.t
    )


def set_fast_forward_pull_from_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.set_repository_fast_forward(
        db_conn=db_conn,
        repo_name=arg_set.repo
    )


def create_repository(repo_name):
    vdb = VersionedDbHelper()

    vdb.create_repository(repo_name=repo_name)


def remove_repository(repo_name):
    vdb = VersionedDbHelper()

    vdb.remove_repository(repo_name=repo_name)


def create_repository_version_folder(arg_set):
    vdb = VersionedDbHelper()

    vdb.create_repository_version_folder(
        repo_name=arg_set.repo,
        version=arg_set.mkv
    )


def create_repository_env_type(arg_set):
    vdb = VersionedDbHelper()

    vdb.create_repository_environment(
            repo_name=arg_set.repo,
            env=arg_set.mkenv
    )


def remove_repository_env_type(arg_set):
    vdb = VersionedDbHelper()

    vdb.remove_repository_environment(
            repo_name=arg_set.repo,
            env=arg_set.rmenv
    )


def set_repository_env_version(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.setenv and arg_set.v:
        vdb.set_repository_environment_version(
                repo_name=arg_set.repo,
                env=arg_set.setenv,
                version=arg_set.v,
        )
    else:
        if arg_set.setenv is None:
            error_message(f"Missing {Const.SET_ENV_ARG}")
        if arg_set.v is None:
            error_message(f"Missing {Const.V_ARG}")


def set_repository_include_schema(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.include_schema:
        vdb.add_repository_include_schemas(
                repo_name=arg_set.repo,
                include_schemas=arg_set.include_schema,
        )
    else:
        error_message(f"Missing {Const.INCLUDE_SCHEMA_ARG}")


def remove_repository_include_schema(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.rminclude_schema:
        vdb.remove_repository_include_schemas(
                repo_name=arg_set.repo,
                rminclude_schemas=arg_set.rminclude_schema,
        )
    else:
        error_message(f"Missing {Const.RMINCLUDE_SCHEMA_ARG}")


def set_repository_exclude_schema(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.exclude_schema:
        vdb.add_repository_exclude_schemas(
                repo_name=arg_set.repo,
                exclude_schemas=arg_set.exclude_schema,
        )
    else:
        error_message(f"Missing {Const.EXCLUDE_SCHEMA_ARG}")


def remove_repository_rmexclude_schema(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.rmexclude_schema:
        vdb.remove_repository_exclude_schemas(
                repo_name=arg_set.repo,
                rmexclude_schemas=arg_set.rmexclude_schema,
        )
    else:
        error_message(f"Missing {Const.EXCLUDE_SCHEMA_ARG}")


def set_repository_include_table(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.include_table:
        vdb.add_repository_include_table(
                repo_name=arg_set.repo,
                include_tables=arg_set.include_table,
        )
    else:
        error_message(f"Missing {Const.INCLUDE_TABLE_ARG}")


def remove_repository_include_table(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.rminclude_table:
        vdb.remove_repository_include_table(
                repo_name=arg_set.repo,
                rminclude_table=arg_set.rminclude_table,
        )
    else:
        error_message(f"Missing {Const.RMINCLUDE_SCHEMA_ARG}")


def set_repository_exclude_table(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.exclude_table:
        vdb.add_repository_exclude_table(
                repo_name=arg_set.repo,
                exclude_tables=arg_set.exclude_table,
        )
    else:
        error_message(f"Missing {Const.INCLUDE_TABLE_ARG}")


def remove_repository_exclude_table(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.rmexclude_table:
        vdb.remove_repository_exclude_table(
                repo_name=arg_set.repo,
                rmexclude_table=arg_set.rmexclude_table,
        )
    else:
        error_message(f"Missing {Const.RMEXCLUDE_SCHEMA_ARG}")


def show_version():
    pkg = pkg_resources.require(Const.PGVCTRL)[0]
    information_message(f"{pkg.project_name}: {pkg.version}")


def create_config_file():
    vdb = VersionedDbHelper()
    vdb.create_config()


class DbVctrl(object):
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def run(arg_set):

        try:
            if arg_set.rl:
                # -rl
                display_repo_list()
            elif arg_set.rlv:
                # -rlv
                display_repo_list(verbose=True)
            elif arg_set.chkver:
                # -chkver -repo test_db -d postgresPlay
                check_db_version_on_server(arg_set)
            elif arg_set.init:
                # -init -repo test_db -d postgresPlay [-setenv test]
                initialize_versioned_db(arg_set)
            elif arg_set.mkconf:
                # -mkconf
                create_config_file()
            elif arg_set.mkrepo:
                # -mkrepo test_db
                create_repository(arg_set.mkrepo)
            elif arg_set.rmrepo:
                # -rmrepo test_db
                remove_repository(arg_set.rmrepo)
            elif arg_set.mkv:
                # -mkv 2.0.0.new_version -repo test_db
                create_repository_version_folder(arg_set)
            elif arg_set.mkenv:
                # -mkenv test -repo test_db
                create_repository_env_type(arg_set)
            elif arg_set.rmenv:
                # -rmenv test -repo test_db
                remove_repository_env_type(arg_set)
            elif arg_set.setenv:
                # -setenv test -repo test_db -v 1.0.0
                set_repository_env_version(arg_set)
            elif arg_set.include_schema:
                # --include-schema membership -repo pgvctrl_test
                set_repository_include_schema(arg_set)
            elif arg_set.rminclude_schema:
                # --rminclude-schema membership -repo pgvctrl_test
                remove_repository_include_schema(arg_set)
            elif arg_set.exclude_schema:
                # --exclude-schema membership -repo pgvctrl_test
                set_repository_exclude_schema(arg_set)
            elif arg_set.rmexclude_schema:
                # --rmexclude-schema membership -repo pgvctrl_test
                remove_repository_rmexclude_schema(arg_set)
            elif arg_set.include_table:
                # --include-table membership.user_type -repo pgvctrl_test
                set_repository_include_table(arg_set)
            elif arg_set.rminclude_table:
                # --rminclude-table membership.user_type -repo pgvctrl_test
                remove_repository_include_table(arg_set)
            elif arg_set.exclude_table:
                # --exclude-table membership.user_type -repo pgvctrl_test
                set_repository_exclude_table(arg_set)
            elif arg_set.rmexclude_table:
                # --rmexclude-table membership.user_type -repo pgvctrl_test
                remove_repository_exclude_table(arg_set)
            elif arg_set.apply:
                # -apply <-v 0.1.0 | -env test> -repo test_db -d postgresPlay
                apply_repository_to_db(arg_set)
            elif arg_set.setff:
                # -setff -repo test_db -d postgresPlay
                set_fast_forward_pull_from_db(arg_set)
            elif arg_set.applyff:
                # -applyff 0.1.0.BaseDeploy -repo test_db -d postgresPlay
                apply_fast_forward_to_db(arg_set)
            elif arg_set.pulldata:
                # -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay
                pull_table_for_repo_data(arg_set)
            elif arg_set.pushdata:
                # -pushdata -repo test_db -d postgresPlay
                push_repo_data_to_db(arg_set)
            elif arg_set.version:
                # -version
                show_version()
            else:
                prj_name = pkg_resources.require(Const.PGVCTRL)[0].project_name
                error_message(f"{prj_name}: No operation specified\nTry \"{prj_name} --help\" for more information.")
        except VersionedDbExceptionProductionChangeNoProductionFlag as e:
            error_message(e.message)
        except VersionedDbException as e:
            error_message(e.message)
        except KeyError as e:
            error_message(f"Invalid key in config, expected {e}")
        except ProcessExecutionError as e:
            error_message(f"DB Error {e.stderr}")
        except OSError as e:
            error_message(f"OSError: {e.strerror} ({e.filename})")


def main():
    arg_set = parse_args(sys.argv[1:])
    c = DbVctrl()
    c.run(arg_set)


if __name__ == '__main__':
    main()
