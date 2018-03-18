import argparse
# TODO: make arguments dependent as needed
from plumbum import ProcessExecutionError
import pkg_resources

from dbversioning.errorUtil import VersionedDbException, VersionedDbExceptionProductionChangeNoProductionFlag
from dbversioning.versionedDbConnection import connection_list
from dbversioning.versionedDbShellUtil import information_message, error_message
from dbversioning.versionedDbUtil import VersionedDbHelper


parent2 = argparse.ArgumentParser(add_help=False)
parent2.add_argument('-set2', action="store_true")

parser = argparse.ArgumentParser(description='Postgres db version control.')
group = parser.add_mutually_exclusive_group()
group.add_argument('-version', help='Show vbctrl version number', action='store_true')
group.add_argument("-init", help='initialize database on server for version control', action='store_true')
group.add_argument('-repolist', help='Show list of repositories', action='store_true')
group.add_argument('-chkver', help='Check database version', action='store_true')
group.add_argument('-mkconf', help='Create dbRepoConfig.json', action='store_true')
group.add_argument('-apply', help='Apply sql version', action='store_true')
group.add_argument('-setff', help='Set version fast forward', action='store_true')
group.add_argument('-applyff', help='Apply version fast forward')
group.add_argument('-pulldata', help='Pull data from repository by table', action='store_true')
group.add_argument('-pushdata', help='Push data from repository to database', action='store_true')


parser.add_argument('-force', help='Force push data from repository to database', action='store_true')

parser.add_argument('-verbose', help='Verbose output', action='store_true')
parser.add_argument('-t', metavar='', help='Pull table for data', action='append')

parser.add_argument('-v', metavar='', help='Version number')
parser.add_argument('-mkv', metavar='', help='Make version number')
parser.add_argument('-mkenv', metavar='', help='Make environment type')
parser.add_argument('-repo', metavar='', help='Repository Database Name')
parser.add_argument('-production', help='Database production flag', action='store_true')

parser.add_argument('-svc', metavar='', help='pg service')

parser.add_argument('-d', metavar='', help='database name on server')
parser.add_argument('-host', metavar='', help='postgres server host')
parser.add_argument('-p', metavar='', help='port')
parser.add_argument('-u', metavar='', help='database username')
parser.add_argument('-pwd', metavar='', help='password')


def display_repo_list(arg_set):
    if arg_set.repolist:
        c = VersionedDbHelper()
        verbose = True if arg_set.verbose else False
        c.display_repo_list(verbose)


def check_db_version_on_server(arg_set):
    c = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    c.display_db_version_on_server(db_conn, arg_set.repo)


def initialize_versioned_db(arg_set):
    init = VersionedDbHelper()
    db_conn = connection_list(arg_set)
    
    init.initialize_db_version_on_server(db_conn=db_conn, repo_name=arg_set.repo, is_production=arg_set.production)


def apply_repository_to_db(arg_set):
    vdb = VersionedDbHelper()
    db_conn = connection_list(arg_set)

    vdb.apply_repository_to_database(
        db_conn=db_conn,
        repo_name=arg_set.repo,
        version=arg_set.v,
        is_production=arg_set.production
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
            if arg_set.repolist:
                # -repolist -verbose
                display_repo_list(arg_set)
            elif arg_set.chkver:
                # -chkver -repo test_db -d postgresPlay
                check_db_version_on_server(arg_set)
            elif arg_set.init:
                # -init -repo test_db -d postgresPlay
                initialize_versioned_db(arg_set)
            elif arg_set.mkconf:
                # -mkconf
                create_config_file()
            elif arg_set.mkv:
                # -mkv 2.0.new_version -repo test_db
                create_repository_version_folder(arg_set)
            elif arg_set.mkenv:
                # -mkenv test -repo test_db
                create_repository_env_type(arg_set)
            elif arg_set.apply:
                # -apply -v 0.1 -repo test_db -d postgresPlay
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
        except Exception as e:
            error_message(e)


def main():
    c = DbVctrl()
    c.run()


if __name__ == '__main__':
    main()
