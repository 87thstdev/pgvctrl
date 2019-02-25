APPLY_ARG = "-apply"
APPLY_SS_ARG = "-applyss"
APPLY_SS_LONG_ARG = "-apply-schema-snapshot"
CHECK_VER_ARG = "-chkver"
ENV_ARG = "-env"
FORCE_ARG = "-force"
INIT_ARG = "-init"
LIST_SS_ARG = "-lss"
LIST_SS_LONG_ARG = "-list-schema-snapshots"
LIST_DD_ARG = "-ldd"
LIST_DD_LONG_ARG = "-list-database-dumps"
LIST_REPOS_ARG = "-lr"
LIST_REPOS_VERBOSE_ARG = "-lrv"
MAKE_ENV_ARG = "-mkenv"
MAKE_REPO_ARG = "-mkrepo"
MAKE_V_ARG = "-mkv"
MKCONF_ARG = "-mkconf"
PRODUCTION_ARG = "-production"
PULL_DATA_ARG = "-pulldata"
PUSH_DATA_ARG = "-pushdata"
DUMP_ARG = "-dump"
RESTORE_ARG = "-restore"
REMOVE_ENV_ARG = "-rmenv"
REMOVE_REPO_ARG = "-rmrepo"
REPO_ARG = "-repo"
STATUS = "-status"
SERVICE_ARG = "-svc"
SET_ENV_ARG = "-setenv"
SET_VERSION_STORAGE_TABLE_OWNER_ARG = "-set-version-storage-owner"
GETSS_ARG = "-getss"
TIMER_OFF_ARG = "-timer-off"
TIMER_ON_ARG = "-timer-on"
DATA_TBL_ARG = "-dt"
DATA_TBL_LONG_ARG = "-data-table"
VERSION_ARG = "-version"
V_ARG = "-v"

DATABASE_ARG = "-d"
HOST_ARG = "-host"
PORT_ARG = "-p"
USER_ARG = "-u"
DB_TABLE_ARG = "-t"

PSQL_HOST_PARAM = "-h"
PSQL_USER_PARAM = "-U"

PGVCTRL = "pgvctrl"

INCLUDE_SCHEMA_ARG = "-n"
INCLUDE_SCHEMA_LONG_ARG = "-schema"
INCLUDE_TABLE_ARG = "-t"
INCLUDE_TABLE_LONG_ARG = "-table"
EXCLUDE_SCHEMA_ARG = "-N"
EXCLUDE_SCHEMA_LONG_ARG = "-exclude-schema"
EXCLUDE_TABLE_ARG = "-T"
EXCLUDE_TABLE_LONG_ARG = "-exclude-table"
RMINCLUDE_SCHEMA_ARG = "-rmn"
RMINCLUDE_SCHEMA_LONG_ARG = "-rm-schema"
RMINCLUDE_TABLE_ARG = "-rmt"
RMINCLUDE_TABLE_LONG_ARG = "-rm-table"
RMEXCLUDE_SCHEMA_ARG = "-rmN"
RMEXCLUDE_SCHEMA_LONG_ARG = "-rm-exclude-schema"
RMEXCLUDE_TABLE_ARG = "-rmT"
RMEXCLUDE_TABLE_LONG_ARG = "-rm-exclude-table"

PG_INCLUDE_SCHEMA_ARG = "--schema"
PG_EXCLUDE_SCHEMA_ARG = "--exclude-schema"
PG_INCLUDE_TABLE_ARG = "--table"
PG_EXCLUDE_TABLE_ARG = "--exclude-table"

PUSHING_DATA = "Pushing Data"

DATA_TABLE = "table"
DATA_COLUMN_INSERTS = "column-inserts"
DATA_APPLY_ORDER = "apply-order"
DATA_PRE_PUSH_FILE = "_pre_push.sql"
DATA_POST_PUSH_FILE = "_post_push.sql"

TAB = "    "
TABS = f"{TAB}{TAB}"

KB = float(1024)
MB = float(KB ** 2)
GB = float(KB ** 3)
TB = float(KB ** 4)

MINIMUM_SECONDS = 0.005
SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 60 ** 2
