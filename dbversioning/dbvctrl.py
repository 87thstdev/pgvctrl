import argparse

from plumbum import ProcessExecutionError
import pkg_resources

from dbversioning.errorUtil import (
    VersionedDbException,
    VersionedDbExceptionProductionChangeNoProductionFlag)
from dbversioning.versionedDbConnection import connection_list
from dbversioning.versionedDbShellUtil import (
    information_message,
    error_message)
from dbversioning.versionedDbUtil import VersionedDbHelper


parser = argparse.ArgumentParser(description='Postgres db version control.')
group = parser.add_mutually_exclusive_group()
group.add_argument('-version', help='Show vbctrl version number', action='store_true')
group.add_argument("-init", help='Initialize database on server for version control', action='store_true')
group.add_argument('-rl', help='List repositories', action='store_true')
group.add_argument('-rlv', help='List repositories verbose', action='store_true')
group.add_argument('-chkver', help='Check database version', action='store_true')
group.add_argument('-mkconf', help='Create dbRepoConfig.json', action='store_true')
group.add_argument('-apply', help='Apply sql version', action='store_true')
group.add_argument('-setff', help='Set version fast forward', action='store_true')
group.add_argument('-applyff', help='Apply version fast forward')
group.add_argument('-pulldata', help='Pull data from repository by table', action='store_true')
group.add_argument('-pushdata', help='Push data from repository to database', action='store_true')


parser.add_argument('-force', help='Force push data from repository to database', action='store_true')

parser.add_argument('-t', metavar='', help='Pull table for data', action='append')

parser.add_argument('-v', metavar='', help='Version number')
parser.add_argument('-mkrepo', metavar='', help='Make Repository')
parser.add_argument('-rmrepo', metavar='', help='Remove Repository')
parser.add_argument('-mkv', metavar='', help='Make version number')
parser.add_argument('-mkenv', metavar='', help='Make environment type')
parser.add_argument('-rmenv', metavar='', help='Remove environment type')
parser.add_argument('-setenv', metavar='', help='Set environment type to a version')
parser.add_argument('-env', metavar='', help='Repository environment name')
parser.add_argument('-repo', metavar='', help='Repository name')
parser.add_argument('-production', help='Database production flag', action='store_true')

parser.add_argument('-svc', metavar='', help='pg service')

parser.add_argument('-d', metavar='', help='database name on server')
parser.add_argument('-host', metavar='', help='postgres server host')
parser.add_argument('-p', metavar='', help='port')
parser.add_argument('-u', metavar='', help='database username')
parser.add_argument('-pwd', metavar='', help='password')


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
            error_message(f"Missing -setenv")
        if arg_set.v is None:
            error_message(f"Missing -v")


def show_version():
    pkg = pkg_resources.require("pgvctrl")[0]
    information_message("{0}: {1}".format(pkg.project_name, pkg.version))


def create_config_file():
    vdb = VersionedDbHelper()
    vdb.create_config()


class DbVctrl(object):
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def run():
        arg_set = parser.parse_args()

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
                # -mkv 2.0.new_version -repo test_db
                create_repository_version_folder(arg_set)
            elif arg_set.mkenv:
                # -mkenv test -repo test_db
                create_repository_env_type(arg_set)
            elif arg_set.rmenv:
                # -rmenv test -repo test_db
                remove_repository_env_type(arg_set)
            elif arg_set.setenv:
                # -setenv test -repo test_db -v 1.0
                set_repository_env_version(arg_set)
            elif arg_set.apply:
                # -apply <-v 0.1 | -env test> -repo test_db -d postgresPlay
                apply_repository_to_db(arg_set)
            elif arg_set.setff:
                # -setff -repo test_db -d postgresPlay
                set_fast_forward_pull_from_db(arg_set)
            elif arg_set.applyff:
                # -applyff 0.1.BaseDeploy -repo test_db -d postgresPlay
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
                prj_name = pkg_resources.require("pgvctrl")[0].project_name
                information_message("{0}: No operation specified\nTry \"{0} --help\" "
                                    "for more information.".format(prj_name))
        except VersionedDbExceptionProductionChangeNoProductionFlag as e:
            error_message(e.message)
        except VersionedDbException as e:
            error_message(e.message)
        except KeyError as e:
            error_message("Invalid key in config, expected {0}".format(e))
        except ProcessExecutionError as e:
            error_message("DB Error {0}".format(e.stderr))
        except OSError as e:
            error_message("OSError: {0} ({1})".format(e.strerror, e.filename))


def main():
    c = DbVctrl()
    c.run()


if __name__ == '__main__':
    main()
