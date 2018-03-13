class VersionedDbException(Exception):
    def __init__(self, message=None):
        self.message = "General VersionedDbException: {0}".format(message)
        pass


class VersionedDbExceptionFileExits(VersionedDbException):
    def __init__(self, file_name):
        self.message = "File already exists: {0}".format(file_name)
        pass


class VersionedDbExceptionRepoVersionExits(VersionedDbException):
    def __init__(self, repo_name, version):
        self.message = "Repository version already exists: {0} {1}.{2}".format(
            repo_name, version.major, version.minor
        )
        pass


class VersionedDbExceptionRepoEnvExits(VersionedDbException):
    def __init__(self, repo_name, env):
        self.message = f"Repository environment already exists: {repo_name} {env}"
        pass


class VersionedDbExceptionRepoDoesNotExits(VersionedDbException):
    def __init__(self, repo_name):
        self.message = f"Repository does not exist: {repo_name}"
        pass


class VersionedDbExceptionRepoVersionDoesNotExits(VersionedDbException):
    def __init__(self, repo_name, version_name):
        self.message = "Repository version does not exist: {0} {1}".format(
            repo_name, version_name
        )
        pass


class VersionedDbExceptionProductionChangeNoProductionFlag(VersionedDbException):
    def __init__(self, action_name=None):
        self.message = "Production changes need the -production flag: {0}".format(
            action_name
        )
        pass


class VersionedDbExceptionProductionChangeNotAllowed(VersionedDbException):
    def __init__(self, action_name=None):
        self.message = "Production changes not allowed for: {0}".format(
            action_name
        )
        pass


class VersionedDbExceptionFastForwardNotAllowed(VersionedDbException):
    def __init__(self):
        self.message = "Fast forwards only allowed on empty databases."
        pass


class VersionedDbExceptionMissingArgs(VersionedDbException):
    def __init__(self):
        self.message = "Missing connection args"
        pass


class VersionedDbExceptionFolderMissing(VersionedDbException):
    def __init__(self, folder_name):
        self.message = "Folder missing: {0}".format(folder_name)
        pass


class VersionedDbExceptionFileMissing(VersionedDbException):
    def __init__(self, file_name):
        self.message = "File missing: {0}".format(file_name)
        pass


class VersionedDbExceptionMissingVersionTable(VersionedDbException):
    def __init__(self, table_name):
        self.message = "Missing Versioning Table: {0}".format(table_name)
        pass


class VersionedDbExceptionMissingDataTable(VersionedDbException):
    def __init__(self, table_name):
        self.message = "Missing Data Table: {0}".format(table_name)
        pass


class VersionedDbExceptionBadDateSource(VersionedDbException):
    def __init__(self, dbconn):
        self.message = "Invalid Data Connection: {0}".format(dbconn)
        pass


class VersionedDbExceptionInvalidRepo(VersionedDbException):
    def __init__(self):
        self.message = "Invalid Repo!"
        pass


class VersionedDbExceptionDatabaseAlreadyInit(VersionedDbException):
    def __init__(self):
        self.message = "Database already initialized!"
        pass


class VersionedDbExceptionSqlExecutionError(VersionedDbException):
    def __init__(self, stderr):
        self.message = "Sql Error: {0}".format(stderr)
        pass


class VersionedDbExceptionBadConfigVersionFound(VersionedDbException):
    def __init__(self):
        self.message = "Bad config found!"
        pass


class VersionedDbExceptionBadConfigFile(VersionedDbException):
    def __init__(self):
        self.message = "Bad config file!"
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
        self.message = "Version is higher then applying {0} > {1}!".format(current, new)
        pass
