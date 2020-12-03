1.1.0 / 2020-12-03
==================

  * Added feature -on-server: running on server (no host needed)

    Allows for no -host or -d.


1.1.0 / 2020-10-03
==================

  * Fixed bug: Ensure the following arguments have the -repo argument.

    (-status, -mkv, -init, -mkenv, -rmenv, -chkver, -set-version-storage-owner, -setenv, -n, -schema, -rmn, -rm-schema, -N, -exclude-schema, -rmN, -rm-exclude-schema, -t, -table, -rmt, -rm-table, -T, -exclude-table, -rmT, -rm-exclude-table, -apply , -getss, -applyss, -apply-schema-snapshot, -pulldata, -pushdata, -dump, -restore)

  * Added feature -rmv: Remove version number and files (requires confirmation)

    Removes the version and all the files with it.

  * Added feature: Add --name command for -getss

    Sets Schema Snapshots name

  * Added feature: Add --name command for -dump

    Sets Database Dump name

  * Remove third-party json dependency
