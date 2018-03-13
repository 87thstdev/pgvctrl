import os
from collections import namedtuple

import simplejson as json
from os.path import join

from dbversioning.versionedDbHelper import get_valid_elements
from .errorUtil import (
    VersionedDbExceptionBadConfigVersionFound,
    VersionedDbExceptionFileExits,
    VersionedDbExceptionVersionIsHigherThanApplying,
    VersionedDbExceptionFolderMissing,
    VersionedDbExceptionRepoVersionExits,
    VersionedDbExceptionRepoVersionDoesNotExits,
    VersionedDbExceptionProductionChangeNoProductionFlag,
    VersionedDbExceptionMissingVersionTable,
    VersionedDbExceptionFastForwardNotAllowed, VersionedDbExceptionRepoDoesNotExits)
from .versionedDbShellUtil import (
    VersionDbShellUtil,
    information_message,
    DATA_DUMP_CONFIG_NAME,
    dir_exists)
from .versionedDb import VersionDb, FastForwardDb, GenericSql
from .repositoryconf import (
    RepositoryConf,
    VERSION_STORAGE,
    SNAPSHOTS,
    FAST_FORWARD,
    ROLLBACK_FILE_ENDING)

to_unicode = str

Version_Numbers = namedtuple("version_numbers", ["major", "minor"])

DB_INIT_DISPLAY = "Database initialized"
DB_INIT_PRODUCTION_DISPLAY = DB_INIT_DISPLAY + " (PRODUCTION)"


class VersionedDbHelper:

    @staticmethod
    def __init__():
        pass

    @staticmethod
    def display_repo_list(verbose):
        """
        :return: list of database in config
        """
        db_repos = []
        conf = RepositoryConf()
        root = conf.root()

        ignored = {FAST_FORWARD, SNAPSHOTS}
        repo_locations = get_valid_elements(root, ignored)

        for repo_location in repo_locations:
            db_repos.append(VersionDb(join(os.getcwd(), root, repo_location)))

        for db_repo in db_repos:
            information_message("{0}".format(db_repo.db_name))
            if verbose:
                v_sorted = sorted(db_repo.versions, key=lambda v: (v.major, v.minor))
                for v in v_sorted:
                    information_message("\tv {0}".format(v.full_name))

                    for s in v.sql_files:
                        information_message("\t\t{0} {1}".format(s.number, s.name))

    @staticmethod
    def valid_repository(repository):
        found = True
        conf = RepositoryConf()
        root = conf.root()
        try:
            VersionDb(join(os.getcwd(), root, repository))
        except OSError:
            found = False

        return found

    @staticmethod
    def get_repository_version(repository, version):
        conf = RepositoryConf()
        root = conf.root()

        vdb = VersionDb(join(os.getcwd(), root, repository))

        rtn = [v for v in vdb.versions
               if v.major == version.major and v.minor == version.minor]

        if len(rtn) == 0:
            return None

        return rtn

    @staticmethod
    def create_repository_version(repository, version):
        conf = RepositoryConf()
        root = conf.root()
        vdb = VersionDb(join(os.getcwd(), root, repository))
        if vdb.create_version(version):
            information_message("Version {0}/{1} created.".format(repository, version))

    @staticmethod
    def get_repository_fast_forward_version(repository, version):
        conf = RepositoryConf()

        vdb = FastForwardDb(join(os.getcwd(), conf.fast_forward_dir(), repository))

        rtn = [v for v in vdb.fast_forward_versions if v.full_name == version]

        if len(rtn) == 0:
            return None

        return rtn

    @staticmethod
    def display_db_version_on_server(db_conn, repo_name):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        VersionDbShellUtil.display_db_instance_version(v_stg, db_conn)

    @staticmethod
    def initialize_db_version_on_server(db_conn, repo_name, is_production):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        if VersionDbShellUtil.init_db(repo_name=repo_name, v_stg=v_stg, db_conn=db_conn, is_production=is_production):
            if is_production:
                information_message(DB_INIT_PRODUCTION_DISPLAY)
            else:
                information_message(DB_INIT_DISPLAY)

    @staticmethod
    def apply_repository_fast_forward_to_database(repo_name, db_conn, full_version):
        has_version_tbl = True
        try:
            v_stg = VersionedDbHelper._get_v_stg(repo_name)
            dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)
        except VersionedDbExceptionMissingVersionTable:
            has_version_tbl = False
        
        if (has_version_tbl):
            raise VersionedDbExceptionFastForwardNotAllowed()

        fast_forward_to = VersionedDbHelper.get_repository_fast_forward_version(
            repo_name, full_version
        )
        if fast_forward_to:
            VersionDbShellUtil.apply_fast_forward_sql(db_conn, fast_forward_to[0], repo_name)
        else:
            information_message("Fast forward not found {0}".format(full_version))

    @staticmethod
    def push_data_to_database(repo_name, db_conn, force, is_production):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        if (is_production != dbver.is_production):
            raise VersionedDbExceptionProductionChangeNoProductionFlag('-pushdata')

        conf = RepositoryConf()
        data_files = []
        data_dump = conf.get_data_dump_dir(repo_name)

        if dir_exists(data_dump):
            ignored = {DATA_DUMP_CONFIG_NAME}
            data_file_list = get_valid_elements(conf.get_data_dump_dir(repo_name), ignored)

            for sql in data_file_list:
                sql_path = os.path.join(data_dump, sql)
                gs = GenericSql(sql_path)
                data_files.append(gs)

            VersionedDbHelper.apply_data_sql_files_to_database(db_conn, data_files, force)
        else:
            raise VersionedDbExceptionFolderMissing(data_dump)

    @staticmethod
    def pull_table_for_repo_data(repo_name, db_conn, table_list=None):
        if table_list:
            VersionDbShellUtil.pull_tables_from_database(repo_name, db_conn, table_list)
        else:
            VersionDbShellUtil.pull_repo_tables_from_database(repo_name, db_conn)

    @staticmethod
    def apply_repository_to_database(repo_name, db_conn, version, is_production=False):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        if (is_production != dbver.is_production):
            raise VersionedDbExceptionProductionChangeNoProductionFlag('-apply')
            
        repo_nums = VersionedDbHelper.get_version_numbers(dbver.version)
        ver_nums = VersionedDbHelper.get_version_numbers(version)

        repo_ver = VersionedDbHelper.get_repository_version(repo_name, ver_nums)

        if repo_ver is None:
            raise VersionedDbExceptionRepoVersionDoesNotExits(repo_name, version)

        apply_repo = repo_ver[0]

        standing = VersionedDbHelper._version_standing(ver_nums, repo_nums)
        if standing < 0:
            raise VersionedDbExceptionVersionIsHigherThanApplying(ver_nums, version)

        VersionedDbHelper.apply_sql_files_to_database(db_conn, apply_repo.sql_files)

        # Add snapshot
        if RepositoryConf.auto_snapshots():
            VersionDbShellUtil.dump_version_snapshot(db_conn, v_stg)

        ver_hash = apply_repo.get_version_hash_set()
        VersionDbShellUtil.set_db_instance_version(
            db_conn, v_stg, apply_repo.full_name, ver_hash
        )

        return True

    @staticmethod
    def set_repository_fast_forward(repo_name, db_conn):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)

        VersionDbShellUtil.dump_version_fast_forward(db_conn, v_stg)

    @staticmethod
    def apply_sql_files_to_database(db_conn, sql_files):
        for sql_file in sql_files:
            if not sql_file.path.endswith(ROLLBACK_FILE_ENDING):
                VersionDbShellUtil.apply_sql_file(db_conn, sql_file)

    @staticmethod
    def apply_data_sql_files_to_database(db_conn, sql_files, force=None):
        for sql_file in sql_files:
            VersionDbShellUtil.apply_data_sql_file(db_conn, sql_file, force)

    @staticmethod
    def _version_standing(v_h, v_l):
        if v_h.major > v_l.major:
            return 1

        if v_h.major == v_l.major and v_h.minor > v_l.minor:
            return 1

        if v_h.major == v_l.major and v_h.minor == v_l.minor:
            return 0

        return -1

    @staticmethod
    def create_repository_version_folder(repo_name, version):
        version_nums = VersionedDbHelper.get_version_numbers(version)
        version_found = VersionedDbHelper.get_repository_version(repo_name, version_nums)

        if version_found:
            raise VersionedDbExceptionRepoVersionExits(repo_name, version_found[0])

        VersionedDbHelper.create_repository_version(repo_name, version)

    @staticmethod
    def create_repository_environment(repo_name, env):
        repo_found = VersionedDbHelper.valid_repository(repo_name)
        if not repo_found:
            raise VersionedDbExceptionRepoDoesNotExits(repo_name)

        if RepositoryConf.create_repo_env(repo_name=repo_name, env=env):
            information_message(f"Repository environment created: {repo_name} {env}")
    
    @staticmethod
    def create_config():
        conf = RepositoryConf()
        if not os.path.isfile(conf.config_file_name()):
            with open(conf.config_file_name(), 'w') as outfile:
                str_ = json.dumps(conf.config_json(),
                                  indent=4, sort_keys=True,
                                  separators=(',', ': '), ensure_ascii=True)
                outfile.write(to_unicode(str_))
            information_message("Config file created: {0}".format(conf.config_file_name()))
        else:
            raise VersionedDbExceptionFileExits(conf.config_file_name())

    @staticmethod
    def _get_v_stg(repo_name):
        repo = RepositoryConf.get_repo(repo_name)

        if len(repo) == 1:
            v_stg = repo[0][VERSION_STORAGE]
        elif len(repo) > 1:
            raise VersionedDbExceptionBadConfigVersionFound()
        else:
            conf = RepositoryConf()
            v_stg = conf.default_version_storage()

        return v_stg

    @staticmethod
    def get_version_numbers(version_string):
        ver_array = version_string.split(".")

        return Version_Numbers(int(ver_array[0]), int(ver_array[1]))
