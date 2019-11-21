import dbversioning.dbvctrlConst as Const


class VersionedDbException(Exception):
    def __init__(self, message=None):
        self.message = message
        pass


class VersionedDbExceptionFileExits(VersionedDbException):
    def __init__(self, file_name):
        self.message = f"File already exists: {file_name}"
        pass


class VersionedDbExceptionInvalidSqlName(VersionedDbException):
    def __init__(self, name: str):
        self.message = f"Invalid Sql file name should be [name].sql: {name}"
        pass


class VersionedDbExceptionRepoVersionNumber(VersionedDbException):
    def __init__(self, version: str):
        self.message = f"Repository version number invalid, should be " \
                       f"[Major].[Minor].[Maintenance] at a minimum: {version}"
        pass


class VersionedDbExceptionRepoVersionExits(VersionedDbException):
    def __init__(self, repo_name, version):
        self.message = (
            f"Repository version already exists: {repo_name} "
            f"{version.major}.{version.minor}.{version.maintenance}"
        )
        pass


class VersionedDbExceptionRepoNameInvalid(VersionedDbException):
    def __init__(self, repo_name):
        self.message = f"Repository name invalid {repo_name}"
        pass


class VersionedDbExceptionMissingRepo(VersionedDbException):
    def __init__(self, repo_name):
        self.message = f"Missing repository folder: {repo_name}"
        pass


class VersionedDbExceptionRepoVersionDoesNotExits(VersionedDbException):
    def __init__(self, repo_name, version_name):
        self.message = (
            f"Repository version does not exist: {repo_name} {version_name}"
        )
        pass


class VersionedDbExceptionRepoEnvDoesNotExits(VersionedDbException):
    def __init__(self, repo_name, env):
        self.message = (
            f"Repository environment does not exists: {repo_name} {env}"
        )
        pass


class VersionedDbExceptionEnvDoesMatchDbEnv(VersionedDbException):
    def __init__(self, env, db_env):
        self.message = f"Environment does not match databases environment: {env} != {db_env}"
        pass


class VersionedDbExceptionRepoExits(VersionedDbException):
    def __init__(self, repo_name):
        self.message = f"Repository already exist: {repo_name}"
        pass


class VersionedDbExceptionRepoDoesNotExits(VersionedDbException):
    def __init__(self, repo_name):
        self.message = f"Repository does not exist: {repo_name}"
        pass


class VersionedDbExceptionProductionChangeNoProductionFlag(
    VersionedDbException
):
    def __init__(self, action_name=None):
        self.message = f"Production changes need the {Const.PRODUCTION_ARG} flag: {action_name}"
        pass


class VersionedDbExceptionSchemaSnapshotNotAllowed(VersionedDbException):
    def __init__(self):
        self.message = "Schema Snapshots only allowed on empty databases."
        pass


class VersionedDbExceptionRestoreNotAllowed(VersionedDbException):
    def __init__(self):
        self.message = "Database restores only allowed on empty databases."
        pass


class VersionedDbExceptionMissingArgs(VersionedDbException):
    def __init__(self):
        self.message = "Missing connection args"
        pass


class VersionedDbExceptionFileMissing(VersionedDbException):
    def __init__(self, file_name):
        self.message = f"File missing: {file_name}"
        pass


class VersionedDbExceptionMissingVersionTable(VersionedDbException):
    def __init__(self, table_name):
        self.message = f"Missing Versioning Table: {table_name}"
        pass


class VersionedDbExceptionBadDateSource(VersionedDbException):
    def __init__(self, dbconn):
        self.message = f"Invalid Data Connection: {dbconn}"
        pass


class VersionedDbExceptionIncludeExcludeSchema(VersionedDbException):
    def __init__(self, repo: str):
        self.message = f"{repo} cannot have both included and excluded schemas!"
        pass


class VersionedDbExceptionDatabaseAlreadyInit(VersionedDbException):
    def __init__(self):
        self.message = "Database already initialized!"
        pass


class VersionedDbExceptionSqlNamingError(VersionedDbException):
    def __init__(self, filename):
        self.message = f"Sql filename error: {filename}"
        pass


class VersionedDbExceptionSqlExecutionError(VersionedDbException):
    def __init__(self, stderr):
        self.message = f"Sql Error: {stderr}"
        pass


class VersionedDbExceptionBadConfigMultiRepos(VersionedDbException):
    def __init__(self, repo):
        self.message = f"Bad config: Multiple repositories found for {repo}!"
        pass


class VersionedDbExceptionBadDataConfigFile(VersionedDbException):
    def __init__(self):
        self.message = "Bad data config file!"
        pass


class VersionedDbExceptionNoVersionFound(VersionedDbException):
    def __init__(self):
        self.message = "No version found!"
        pass


class VersionedDbExceptionTooManyVersionRecordsFound(VersionedDbException):
    def __init__(self):
        self.message = "Too many version records found!"
        pass


class VersionedDbExceptionVersionIsHigherThanApplying(VersionedDbException):
    def __init__(self, current, new):
        self.message = f"Version is higher then applying {current} > {new}!"
        pass
