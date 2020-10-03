import argparse
import sys
from typing import List

from plumbum import ProcessExecutionError
import pkg_resources
import dbversioning.dbvctrlConst as Const
from dbversioning.errorUtil import (
    VersionedDbException,
)
from dbversioning.versionedDbConnection import connection_list
from dbversioning.versionedDbShellUtil import information_message, error_message, error_message_non_terminating
from dbversioning.versionedDbUtil import VersionedDbHelper


def formatter(prog):
    return argparse.HelpFormatter(prog, max_help_position=50, width=120)


def parse_args(args):

    parser = argparse.ArgumentParser(
            description="Postgres db version control.",
            formatter_class=formatter
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
            Const.VERSION_ARG,
            help="Show pgvctrl version number",
            action="store_true",
    )
    group.add_argument(
            Const.INIT_ARG,
            help="Initialize database on server for version control",
            action="store_true"
    )
    group.add_argument(
            Const.LIST_REPOS_ARG,
            help="List repositories",
            action="store_true"
    )
    group.add_argument(
            Const.LIST_REPOS_VERBOSE_ARG,
            help="List repositories verbose",
            action="store_true",
    )
    group.add_argument(
            Const.LIST_SS_ARG,
            Const.LIST_SS_LONG_ARG,
            help="List repository Schema Snapshots",
            action="store_true"
    )
    group.add_argument(
            Const.LIST_DD_ARG,
            Const.LIST_DD_LONG_ARG,
            help="List repository data dumps",
            action="store_true"
    )
    group.add_argument(
            Const.CHECK_VER_ARG,
            help="Check database version",
            action="store_true"
    )
    group.add_argument(
            Const.STATUS,
            help="Check database repository version status",
            action="store_true"
    )
    group.add_argument(
            Const.MKCONF_ARG,
            help="Create dbRepoConfig.json",
            action="store_true"
    )
    group.add_argument(
            Const.APPLY_ARG,
            help="Apply sql version",
            action="store_true"
    )
    group.add_argument(
            Const.GETSS_ARG,
            help="Get version schema snapshot",
            action="store_true"
    )
    group.add_argument(
            Const.APPLY_SS_ARG,
            Const.APPLY_SS_LONG_ARG,
            metavar="NAME",
            help="Apply version schema snapshot",
    )
    group.add_argument(
            Const.PULL_DATA_ARG,
            help="Pull data from repository by table",
            action="store_true",
    )
    group.add_argument(
            Const.PUSH_DATA_ARG,
            help="Push data from repository to database",
            action="store_true",
    )
    parser.add_argument(
            Const.DATA_TBL_ARG,
            Const.DATA_TBL_LONG_ARG,
            metavar="TABLE_NAME",
            help="Pull/Push table for data",
            action="append"
    )
    parser.add_argument(
            Const.FORCE_ARG,
            help="Force push data from repository to database",
            action="store_true",
    )
    group.add_argument(
            Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
            metavar="OWNER",
            help="Set postgres owner for the version storage table",
    )
    group.add_argument(
            Const.TIMER_ON_ARG,
            help="Turn executions timer on (-apply, -applyss, -pulldata, -pushdata, -dump, -restore)",
            action="store_true",
    )
    group.add_argument(
            Const.TIMER_OFF_ARG,
            help="Turn executions timer on (-apply, -applyss, -pulldata, -pushdata, -dump, -restore)",
            action="store_true",
    )
    group.add_argument(
            Const.DUMP_ARG,
            help="Dump database from server to local file (requires confirmation)",
            action="store_true",
    )
    group.add_argument(
            Const.RESTORE_ARG,
            help="Restore database dump from file to server (requires confirmation)",
            metavar="FILE_NAME",
    )
    parser.add_argument(
            Const.NAME_ARG,
            help=f"Name out file for ({Const.GETSS_ARG}, {Const.DUMP_ARG})",
            metavar="NAME",
    )
    parser.add_argument(
            Const.V_ARG,
            metavar="VERSION_NUMBER",
            help="Version number"
    )
    group.add_argument(
            Const.MAKE_REPO_ARG,
            metavar="REPO_NAME",
            help="Make Repository"
    )
    group.add_argument(
            Const.REMOVE_REPO_ARG,
            metavar="REPO_NAME",
            help="Remove Repository"
    )
    group.add_argument(
            Const.MAKE_V_ARG,
            metavar="VERSION_NUMBER",
            help="Make version number"
    )
    group.add_argument(
            Const.REMOVE_V_ARG,
            metavar="VERSION_NUMBER",
            help="Remove version number and files (requires confirmation)"
    )
    group.add_argument(
            Const.MAKE_ENV_ARG,
            metavar="ENV_NAME",
            help="Make environment type"
    )
    group.add_argument(
            Const.REMOVE_ENV_ARG,
            metavar="ENV_NAME",
            help="Remove environment type"
    )
    parser.add_argument(
            Const.SET_ENV_ARG,
            metavar="ENV_NAME",
            help="Set environment type to a version"
    )
    parser.add_argument(
            Const.ENV_ARG,
            metavar="ENV_NAME",
            help="Repository environment name"
    )
    parser.add_argument(
            Const.REPO_ARG,
            metavar="REPO_NAME",
            help="Repository name"
    )
    parser.add_argument(
            Const.PRODUCTION_ARG,
            help="Database production flag",
            action="store_true"
    )

    parser.add_argument(
            Const.SERVICE_ARG,
            metavar="PG_SERVICE_NAME",
            help="pg service"
    )

    parser.add_argument(
            Const.DATABASE_ARG,
            metavar="DB_NAME",
            help="database name on server"
    )
    parser.add_argument(
            Const.HOST_ARG,
            metavar="HOSTNAME",
            help="postgres server host"
    )
    parser.add_argument(
            Const.PORT_ARG,
            metavar="PORT",
            help="port"
    )
    parser.add_argument(
            Const.USER_ARG,
            metavar="USERNAME",
            help="database username"
    )

    group.add_argument(
            Const.INCLUDE_SCHEMA_ARG,
            Const.INCLUDE_SCHEMA_LONG_ARG,
            metavar="SCHEMA",
            help="Add schema(s) to include schema list",
            action="append",
    )
    group.add_argument(
            Const.INCLUDE_TABLE_ARG,
            Const.INCLUDE_TABLE_LONG_ARG,
            metavar="TABLE",
            help="Add table(s) to include table list",
            action="append",
    )
    group.add_argument(
            Const.EXCLUDE_SCHEMA_ARG,
            Const.EXCLUDE_SCHEMA_LONG_ARG,
            metavar="SCHEMA",
            help="Add schema(s) to exclude schema list",
            action="append",
    )
    group.add_argument(
            Const.EXCLUDE_TABLE_ARG,
            Const.EXCLUDE_TABLE_LONG_ARG,
            metavar="TABLE",
            help="Add table(s) to exclude table list",
            action="append",
    )
    group.add_argument(
            Const.RMINCLUDE_SCHEMA_ARG,
            Const.RMINCLUDE_SCHEMA_LONG_ARG,
            metavar="SCHEMA",
            help="Remove schema(s) from include schema list",
            action="append",
    )
    group.add_argument(
            Const.RMINCLUDE_TABLE_ARG,
            Const.RMINCLUDE_TABLE_LONG_ARG,
            metavar="TABLE",
            help="Remove table(s) from include table list",
            action="append",
    )
    group.add_argument(
            Const.RMEXCLUDE_TABLE_ARG,
            Const.RMEXCLUDE_TABLE_LONG_ARG,
            metavar="TABLE",
            help="Remove table(s) from exclude table list",
            action="append",
    )
    group.add_argument(
            Const.RMEXCLUDE_SCHEMA_ARG,
            Const.RMEXCLUDE_SCHEMA_LONG_ARG,
            metavar="SCHEMA",
            help="Remove schema(s) from exclude schema list",
            action="append",
    )

    return _validate_args(parser, args)


def _validate_args(parser, args):
    parsed_args = parser.parse_args(args)

    if parsed_args.status and (parsed_args.repo is None):
        parser.error(f"{Const.STATUS} requires {Const.REPO_ARG}.")

    if parsed_args.mkv and (parsed_args.repo is None):
        parser.error(f"{Const.MAKE_V_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.rmv and (parsed_args.repo is None):
        parser.error(f"{Const.REMOVE_V_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.init and (parsed_args.repo is None):
        parser.error(f"{Const.INIT_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.mkenv and (parsed_args.repo is None):
        parser.error(f"{Const.MAKE_ENV_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.rmenv and (parsed_args.repo is None):
        parser.error(f"{Const.REMOVE_ENV_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.chkver and (parsed_args.repo is None):
        parser.error(f"{Const.CHECK_VER_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.set_version_storage_owner and (parsed_args.repo is None):
        parser.error(f"{Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.setenv and (parsed_args.repo is None):
        parser.error(f"{Const.SET_ENV_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.n and (parsed_args.repo is None):
        parser.error(f"[{Const.INCLUDE_SCHEMA_ARG} | {Const.INCLUDE_SCHEMA_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.rmn and (parsed_args.repo is None):
        parser.error(f"[{Const.RMINCLUDE_SCHEMA_ARG} | {Const.RMINCLUDE_SCHEMA_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.N and (parsed_args.repo is None):
        parser.error(f"[{Const.EXCLUDE_SCHEMA_ARG} | {Const.EXCLUDE_SCHEMA_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.rmN and (parsed_args.repo is None):
        parser.error(f"[{Const.RMEXCLUDE_SCHEMA_ARG} | {Const.RMEXCLUDE_SCHEMA_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.t and (parsed_args.repo is None):
        parser.error(f"[{Const.INCLUDE_TABLE_ARG} | {Const.INCLUDE_TABLE_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.rmt and (parsed_args.repo is None):
        parser.error(f"[{Const.RMINCLUDE_TABLE_ARG} | {Const.RMINCLUDE_TABLE_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.T and (parsed_args.repo is None):
        parser.error(f"[{Const.EXCLUDE_TABLE_ARG} | {Const.EXCLUDE_TABLE_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.rmT and (parsed_args.repo is None):
        parser.error(f"[{Const.RMEXCLUDE_TABLE_ARG} | {Const.RMEXCLUDE_TABLE_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.apply and (parsed_args.repo is None):
        parser.error(f"{Const.APPLY_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.getss and (parsed_args.repo is None):
        parser.error(f"{Const.GETSS_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.applyss and (parsed_args.repo is None):
        parser.error(f"[{Const.APPLY_SS_ARG} | {Const.APPLY_SS_LONG_ARG}] requires {Const.REPO_ARG}.")

    if parsed_args.pulldata and (parsed_args.repo is None):
        parser.error(f"{Const.PULL_DATA_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.pushdata and (parsed_args.repo is None):
        parser.error(f"{Const.PUSH_DATA_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.dump and (parsed_args.repo is None):
        parser.error(f"{Const.DUMP_ARG} requires {Const.REPO_ARG}.")

    if parsed_args.restore and (parsed_args.repo is None):
        parser.error(f"{Const.RESTORE_ARG} requires {Const.REPO_ARG}.")

    return parsed_args


# <editor-fold desc="arg_calls">
def display_repo_list(verbose=False):
    c = VersionedDbHelper()
    if not c.display_repo_list(verbose):
        error_message_non_terminating("No Repositories available.")


def display_repo_ss_list():
    c = VersionedDbHelper()
    if not c.display_repo_ss_list():
        error_message_non_terminating("No Schema Snapshots available.")


def display_repo_dd_list():
    c = VersionedDbHelper()
    if not c.display_repo_dd_list():
        error_message_non_terminating("No database dumps available.")


def display_repo_status(arg_set):
    c = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    c.display_db_version_status_on_server(db_conn, arg_set.repo)


def check_db_version_on_server(arg_set):
    c = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    c.display_db_version_on_server(db_conn, arg_set.repo)


def initialize_versioned_db(arg_set):
    init = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    if arg_set.env:
        error_message(f"Error setting env: Used {Const.ENV_ARG}, did you mean {Const.SET_ENV_ARG}?")

    init.initialize_db_version_on_server(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            is_production=arg_set.production,
            env=arg_set.setenv,
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
            env=arg_set.env,
    )


def _get_repo_version(arg_set):
    if arg_set.v and arg_set.env:
        raise VersionedDbException(
                "Version and environment args are mutually exclusive."
        )

    if arg_set.v:
        return arg_set.v

    vdb = VersionedDbHelper()
    return vdb.get_repository_environment(
            repo_name=arg_set.repo, env=arg_set.env
    )


def apply_schema_snapshot_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.apply_repository_schema_snapshot_to_database(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            full_version=arg_set.applyss
    )


def push_repo_data_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.push_data_to_database(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            force=arg_set.force,
            table_list=arg_set.dt,
            is_production=arg_set.production,
    )


def repo_database_dump(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.repo_database_dump(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            name=arg_set.name,
            is_production=arg_set.production,
    )


def repo_database_restore(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.repo_database_restore(
            db_conn=db_conn,
            repo_name=arg_set.repo,
            file_name=arg_set.restore
    )


def pull_table_for_repo_data(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.pull_table_for_repo_data(db_conn=db_conn, repo_name=arg_set.repo, table_list=arg_set.dt)


def set_schema_snapshot_pull_from_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.set_repository_schema_snapshot(db_conn=db_conn, repo_name=arg_set.repo, name=arg_set.name)


def create_repository(repo_name):
    vdb = VersionedDbHelper()
    vdb.create_repository(repo_name=repo_name)


def remove_repository(repo_name):
    vdb = VersionedDbHelper()
    vdb.remove_repository(repo_name=repo_name)


def create_repository_version_folder(repo_name: str, version: str):
    vdb = VersionedDbHelper()
    vdb.create_repository_version_folder(repo_name=repo_name, version=version)


def remove_repository_version_folder(repo_name: str, version: str):
    vdb = VersionedDbHelper()
    vdb.remove_repository_version_folder(repo_name=repo_name, version=version)


def create_repository_env_type(repo_name: str, env: str):
    vdb = VersionedDbHelper()
    vdb.create_repository_environment(repo_name=repo_name, env=env)


def remove_repository_env_type(repo_name: str, env: str):
    vdb = VersionedDbHelper()
    vdb.remove_repository_environment(repo_name=repo_name, env=env)


def set_repository_version_storage_owner(repo_name: str, owner: str):
    vdb = VersionedDbHelper()
    vdb.set_repository_version_storage_owner(repo_name=repo_name, owner=owner)


def set_timer(state: bool):
    vdb = VersionedDbHelper()
    vdb.set_timer(state=state)


def set_repository_env_version(arg_set):
    vdb = VersionedDbHelper()

    if arg_set.v:
        vdb.set_repository_environment_version(repo_name=arg_set.repo, env=arg_set.setenv, version=arg_set.v)
    else:
        error_message(f"Missing {Const.V_ARG}")


def set_repository_include_schema(repo_name: str, include_schemas: List[str]):
    vdb = VersionedDbHelper()
    vdb.add_repository_include_schemas(repo_name=repo_name, include_schemas=include_schemas)


def remove_repository_include_schema(repo_name: str, rminclude_schema: List[str]):
    vdb = VersionedDbHelper()
    vdb.remove_repository_include_schemas(repo_name=repo_name, rminclude_schemas=rminclude_schema)


def set_repository_exclude_schema(repo_name: str, exclude_schemas: List[str]):
    vdb = VersionedDbHelper()
    vdb.add_repository_exclude_schemas(repo_name=repo_name, exclude_schemas=exclude_schemas)


def remove_repository_rmexclude_schema(repo_name: str, rmexclude_schema: List[str]):
    vdb = VersionedDbHelper()
    vdb.remove_repository_exclude_schemas(repo_name=repo_name, rmexclude_schemas=rmexclude_schema)


def set_repository_include_table(repo_name: str, include_table: List[str]):
    vdb = VersionedDbHelper()
    vdb.add_repository_include_table(repo_name=repo_name, include_tables=include_table)


def remove_repository_include_table(repo_name: str, rminclude_table: List[str]):
    vdb = VersionedDbHelper()
    vdb.remove_repository_include_table(repo_name=repo_name, rminclude_table=rminclude_table)


def set_repository_exclude_table(repo_name: str, exclude_tables: List[str]):
    vdb = VersionedDbHelper()
    vdb.add_repository_exclude_table(repo_name=repo_name, exclude_tables=exclude_tables)


def remove_repository_exclude_table(repo_name: str, rmexclude_table: List[str]):
    vdb = VersionedDbHelper()
    vdb.remove_repository_exclude_table(repo_name=repo_name, rmexclude_table=rmexclude_table)


def show_version():
    pkg = pkg_resources.require(Const.PGVCTRL)[0]
    information_message(f"{pkg.project_name}: {pkg.version}")


def create_config_file():
    vdb = VersionedDbHelper()
    vdb.create_config()


# </editor-fold>


class DbVctrl(object):
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def run(arg_set):

        try:
            if arg_set.lr:
                # -lr
                display_repo_list()
            elif arg_set.lrv:
                # -lrv
                display_repo_list(verbose=True)
            elif arg_set.lss:
                # -lss
                display_repo_ss_list()
            elif arg_set.ldd:
                # -ldd
                display_repo_dd_list()
            elif arg_set.status:
                # -status -repo test_db -d postgresPlay
                display_repo_status(arg_set)
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
                create_repository_version_folder(repo_name=arg_set.repo, version=arg_set.mkv)
            elif arg_set.rmv:
                # -rmv 1.0.0.old_version -repo test_db
                if user_yes_no_query("Do you want to remove the repository version?"):
                    remove_repository_version_folder(repo_name=arg_set.repo, version=arg_set.rmv)
                else:
                    error_message("Repository version removal cancelled.")
            elif arg_set.mkenv:
                # -mkenv test -repo test_db
                create_repository_env_type(repo_name=arg_set.repo, env=arg_set.mkenv)
            elif arg_set.rmenv:
                # -rmenv test -repo test_db
                remove_repository_env_type(repo_name=arg_set.repo, env=arg_set.rmenv)
            elif arg_set.set_version_storage_owner:
                # -set-version-storage-owner dbowner -repo test_db
                set_repository_version_storage_owner(repo_name=arg_set.repo, owner=arg_set.set_version_storage_owner)
            elif arg_set.timer_on:
                # -timer-on
                set_timer(True)
            elif arg_set.timer_off:
                # -timer-off
                set_timer(False)
            elif arg_set.setenv:
                # -setenv test -repo test_db -v 1.0.0
                set_repository_env_version(arg_set)
            elif arg_set.n:
                # [-n | -schema] membership -repo pgvctrl_test
                set_repository_include_schema(repo_name=arg_set.repo, include_schemas=arg_set.n)
            elif arg_set.rmn:
                # [-rmn | -rm-schema] membership -repo pgvctrl_test
                remove_repository_include_schema(repo_name=arg_set.repo, rminclude_schema=arg_set.rmn)
            elif arg_set.N:
                # [-N | -exclude-schema] membership -repo pgvctrl_test
                set_repository_exclude_schema(repo_name=arg_set.repo, exclude_schemas=arg_set.N)
            elif arg_set.rmN:
                # [-rmN | -rmexclude-schema] membership -repo pgvctrl_test
                remove_repository_rmexclude_schema(repo_name=arg_set.repo, rmexclude_schema=arg_set.rmN)
            elif arg_set.t:
                # [-t | -include-table] membership.user_type -repo pgvctrl_test
                set_repository_include_table(repo_name=arg_set.repo, include_table=arg_set.t)
            elif arg_set.rmt:
                # [-rmt | -rminclude-table] membership.user_type -repo pgvctrl_test
                remove_repository_include_table(repo_name=arg_set.repo, rminclude_table=arg_set.rmt)
            elif arg_set.T:
                # [-T | -exclude-table] membership.user_type -repo pgvctrl_test
                set_repository_exclude_table(repo_name=arg_set.repo, exclude_tables=arg_set.T)
            elif arg_set.rmT:
                # [-rmT | -rmexclude-table] membership.user_type -repo pgvctrl_test
                remove_repository_exclude_table(repo_name=arg_set.repo, rmexclude_table=arg_set.rmT)
            elif arg_set.apply:
                # -apply <-v 0.1.0 | -env test> -repo test_db -d postgresPlay
                apply_repository_to_db(arg_set)
            elif arg_set.getss:
                # -getss -repo test_db -d postgresPlay (--name my_ss)
                set_schema_snapshot_pull_from_db(arg_set)
            elif arg_set.applyss:
                # -applyss 0.1.0.BaseDeploy -repo test_db -d postgresPlay
                apply_schema_snapshot_to_db(arg_set)
            elif arg_set.pulldata:
                # -pulldata -dt error_set -dt membership.user_state -repo test_db -d postgresPlay
                pull_table_for_repo_data(arg_set)
            elif arg_set.pushdata:
                # -pushdata [-dt error_set ... ] -repo test_db -d postgresPlay
                push_repo_data_to_db(arg_set)
            elif arg_set.version:
                # -version
                show_version()
            elif arg_set.dump:
                # -dump -repo test_db -d postgresPlay [-production]
                if user_yes_no_query("Do you want to dump the database?"):
                    repo_database_dump(arg_set)
                else:
                    error_message("Dump database cancelled.")
            elif arg_set.restore:
                # -restore dump_file -repo test_db -d postgresPlay
                if user_yes_no_query("Do you want to restore the database?"):
                    repo_database_restore(arg_set)
                else:
                    error_message("Restore database cancelled.")
            else:
                prj_name = pkg_resources.require(Const.PGVCTRL)[0].project_name
                error_message(
                        f'{prj_name}: No operation specified\nTry "{prj_name} --help" for more information.'
                )
        except VersionedDbException as e:
            error_message(e.message)
        except KeyError as e:
            error_message(f"Invalid key in config, expected {e}")
        except ProcessExecutionError as e:
            error_message(f"DB Error {e.stderr}")


def main():
    arg_set = parse_args(sys.argv[1:])
    c = DbVctrl()
    c.run(arg_set)


def user_yes_no_query(question):
    sys.stdout.write('%s [YES/NO]\n' % question)
    return input() == 'YES'


if __name__ == "__main__":
    main()
