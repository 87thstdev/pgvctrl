dbvctrl
=======

Database **dbvctrl** is a tool designed to help deploy changes to
postgres databases. All changes are stored in versioned folders with the
order of execution set by the programmer.

Prerequisites:
--------------

#. `postgres <https://www.postgresql.org/>`__ ;)
#. A general knowledge of postgres sql.
   `tutorial <http://www.postgresqltutorial.com/>`__
#. Python3

Getting started:
----------------

pip install
-----------

.. code-block::

    pip install pgvctrl

github
------

Download or clone the repository

Install
-------

.. code-block::

   python setup.py install

Get help:

.. code-block::

   pgvctrl -h

Get version:

.. code-block::

    pgvctrl -version

Running the tests
-----------------

In the test directory:

.. code-block::

   pytest

.. _getting-started-1:

Getting Started
---------------

#. If you don’t already have a database, create one on your postgres
   server.
#. Create pgvctrl dbRepoConfig:

   1. Make a directory where you want you database repositories to live.

      .. code-block::

         pgvctrl -mkconf

      This will create a dbRepoConfig.json file.

#. Create database repository:

   1. In the same directory as the dbRepoConfig.json file, run:

      .. code-block::

         pgvctrl -mkrepo [repository name]

      e.g

      .. code-block::

         pgvctrl -mkrepo mydb

      Output:

      .. code-block::

         Repository created: mydb

   **What just happened?**\ 

   -  There will be a folder structure: [my dir]/databases/[repository
      name]/ created.
   -  The dbRepoConfig.json file will be updated to reflect the new
      repository.

#. Initialize database repository:

   1. In the same directory as the dbRepoConfig.json file, run:

      .. code-block::

         pgvctrl -init [db connection information] -repo [repository name]

      For production databases:

      .. code-block::

         pgvctrl -init [db connection information] -repo [repository name] -production

      **NOTE:**\  **Database connection information should include at a
      minimum.**

   *Standard Information*

   .. code-block::

      -d [database name on server]

   e.g.

   .. code-block::

      pgvctrl -init -d mylocaldb -repo mydb

   Other information as needed:

   .. code-block::

       -host [postgres server host]
       -p [port]
       -u [database username]
       -pwd [password]

   *Or*

   *Service
   Information*\ `.pg_service <https://www.postgresql.org/docs/9.6/static/libpq-pgservice.html>`__

   .. code-block::

      -svc [pg service information]

   e.g.

   .. code-block::

      pgvctrl -svc mydatabase:test -repo mydb

   **What just happened?**\  After initialization is complete:

   -  There will be a new table in your database named
      repository_version. This is where pgvctrl stores your repository
      name, version number with a version hash for each sql update file,
      environment name, revision (number of times the same version has
      been applied with different sql hash) and production flag.

#. Make repository version for repository: -mkv: Make version number:

   .. code-block::

      pgvctrl -mkv [x.x.x.version_name] -repo [repository name]

   e.g.:

   .. code-block::

      pgvctrl -mkv 1.0.0.my_new_version -repo mydb

   Output:

   .. code-block::

      Version mydb/1.0.0.my_new_version created.

#. Create sql change files in the versioned directory! These files will
   be used to update your database and should have the naming convention
   of: [order number].[change name].sql e.g.: 100.AddedUserTable.sql

   **Notes:**

   * For best results with sql files, wrap all statements in a Transactions.

   .. code-block::

       BEGIN TRANSACTION;
           [Your sql changes] 
       COMMIT;

#. List repositories and changes:

   .. code-block::

      pgvctrl -rl

   Output:

   .. code-block::

      mydb
           v 1.0.0.my_new_version

   Verbose:

   .. code-block::

      pgvctrl -rlv

   Output:

   .. code-block::

      mydb
           v 0.0.0.my_new_version
               100 AddUsersTable


#. List repository Schema Snapshots:

   .. code-block::

      pgvctrl -lss
        or
      pgvctrl -list-schema-snapshots

   Output:

   .. code-block::

      mydb
           1.0.0.my_new_version      5.21 KB

#. List repository database dumps:

   .. code-block::

      pgvctrl -ldd
        or
      pgvctrl -list-database-dumps

   Output:

   .. code-block::

      mydb
           mydb.test.20190101           132.22 MB


#. When you are ready to apply your changes to your database:

   .. code-block::

      pgvctrl -apply -v [version number] -repo [repository name] [db connection information]

   e.g.

   .. code-block::

      pgvctrl -apply -v 0.0.0 -repo mydb -d mylocaldb

   Output:

   .. code-block::

      Running: 100.AddUsersTable
      ...
      Running: 500.AddStatesTable

   **Notes:**

   -  If you are applying changes to a production database, you must use
      the -production flag.

   **What just happened?**

   -  All of the sql files with [number].[change name].sql were ran
      against your database.
   -  The repository_version table was update with the new version hash.

SQL Error handling
~~~~~~~~~~~~~~~~~~

SQL Error handling on -apply In the event of an SQL error, pgvctrl will attempt to run the rollback version of your sql.

e.g

.. code-block::


    100.AddUsers.sql
    100.AddUsers_rollback.sql - rollback file for 100.AddUsers.sql

-  If your rollback file does not exist or fails, the -apply command fails and no sql after the first failing sql file will be ran.
-  If the rollback file succeeds, all other sql files will be ran until all files have been applied if they can be.

Working with environments:
~~~~~~~~~~~~~~~~~~~~~~~~~~

Setting up environment versions in repositories help ensure versions get
deployed to the proper database.

Making and setting environments.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-mkenv: Make environment type:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -mkenv [env_name] -repo [repository name]

e.g.:

.. code-block::

   pgvctrl -mkenv test -repo mydb

Output:

.. code-block::

   Repository environment created: mydb test

-setenv: Set environment type to a version:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -setenv [env_name] -v [x.x] -repo [repository name]

e.g.:

.. code-block::

   pgvctrl -setenv test -v 1.0.0 -repo mydb

Output:

.. code-block::

   Repository environment set: mydb test 1.0.0

-init database with environment:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -init [db connection information] -repo [repository name] -setenv [env_name]

For production databases:

.. code-block::

   pgvctrl -init [db connection information] -repo [repository name] -setenv [env_name] -production

Output:

.. code-block::

   Database initialized environment [env_name]

-apply using -env:
~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -apply -env [env_name] -repo [repository name] [db connection information]

e.g.

.. code-block::

   pgvctrl -apply -env test -repo mydb -d mylocaldb

Output:

.. code-block::

   Running: 100.AddUsersTable
   ...
   Running: 500.AddStatesTable
   Applied: mydb v 1.1.0.MyVersion.0

What else can pgvctrl do?
-------------------------

-chkver: Check the version and repo on a database:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -chkver -repo [repository name] [db connection information]

e.g:

.. code-block::

    pgvctrl -chkver -repo mydb -d mylocaldb

Output:

.. code-block::

   mydb: 0.0.0.first.0

-status: Check database repository version status:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -status -repo [repository name] [db connection information]

e.g:

.. code-block::

    pgvctrl -status -repo mydb -d mylocaldb

Output:

.. code-block::

    mydb
        v 0.0.0.first ['test']
            Applied        100.some_sql
            Not Applied    200.some_sql
            Different      300.some_sql
            Missing        400.some_sql

- Applied (whitish) - The sql file has been applied to the database.
- Not Applied (green)- The sql file has not yet been applied to the database.
- Different (orange) - The sql file has been applied to the database, but the file has been altered/updated.
- Missing (red) - The file had been applied to the database, but was removed from the version.

-timer-on/-timer-off: Turn executions timer on/off for -apply, -applyss, -pulldata, -pushdata, -dump and -restore:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    pgvctrl -timer-on

Output:

.. code-block::

    Execution Timer ON

.. code-block::

    pgvctrl -timer-off

Output:

.. code-block::

    Execution Timer OFF


**What happens?**\

-  The "timeExecutions" value in dbRepoConfig.json is toggled

-rmenv: Remove environment type:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -rmenv [env_name] -repo [repository name]

e.g.:

.. code-block::

   pgvctrl -rmenv test -repo mydb

Output:

.. code-block::

   Repository environment removed: mydb test

-rmrepo: Remove Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   pgvctrl -rmrepo [repository name]

e.g.:

.. code-block::

   pgvctrl -rmrepo test

Output:

.. code-block::

   Repository removed: test

**Notes:**\

* If this command does not remove the folder from database, you must remove it and its contents yourself. This is a safety measure.
* Any repository folders left behind will be displayed as UNREGISTERED when the -rl option is used.

Manage schemas and tables in Schema Snapshots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Manage schemas (–schema, –exclude-schema, –rm-schema, –rm-exclude-schema):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Allows the user to say what schemas structures to include/exclude
   when Schema Snapshots are created.
#. The ‘rm’ arguments allow the user to remove schemas from the included
   and excluded lists.

To include a schema:

.. code-block::

   pgvctrl -n membership -repo pgvctrl_test
     or
   pgvctrl -schema membership -repo pgvctrl_test

Output:

.. code-block::

   Repository added: pgvctrl_test
   include-schemas ['membership']

**What happens?**\ 

-  The dbRepoConfig.json file with have the membership schema added to
   the includeSchemas list property of the “pgvctrl_test” repository

Manage table (–table, –exclude-table, –rm-table, –rmexclude-table):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Allows the user to say what tables structures to include/exclude when
   Schema Snapshots are created.
#. The ‘rm’ arguments allow the user to remove tables from the included
   and excluded lists.

To include a table:

.. code-block::

   pgvctrl -t membership.user -repo pgvctrl_test
     or
   pgvctrl -table membership.user -repo pgvctrl_test

Output:

.. code-block::

   Repository added: pgvctrl_test
   include-table ['membership.user']

**Notes:**

#. If a table/schema is included and then later excluded, the table/schema is moved from included to exclude and vice versa.
#. Include table/schema works the same as with pg_dump.

Schema Snapshot (-getss, -applyss)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**What are Schema Snapshots?**\  Schema Snapshots are snapshots of the
database structure (tables, views, functions ect.) at the time the snapshot was taken.

**Notes:**

#. There can be only one per repository version!
#. The table holding the repository information (repository_version) will be saved as an insert in the Schema Snapshot.
#. Currently, only the schema is saved with Schema Snapshots.
#. If there were database schema changes outside of pgvctrl, it will be captured in the Schema Snapshot.
#. Schema Snapshots should only be applied to empty databases.

-getss: Set version Schema Snapshot
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -getss -repo [repository name] [db connection information]

-applyss or -apply-schema-snapshot: Apply version Schema Snapshot
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -applyss [Schema Snapshot Name] -repo [repository name] [db connection information]

Manage data (-pulldata, -pushdata)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There could be many reason why one would want to manage data:

#. Lookup tables.
#. Testing data.
#. Just because your boss wants you too.

-pulldata: Pull data from repository by table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -pulldata [-dt [table name]] -repo [repository name] [db connection information]

e.g.

.. code-block::

   -pulldata -dt error_set -dt membership.user_state -repo mydb -d mylocaldb

Output:

.. code-block::

   
   Pulling: error_set
   Pulling: membership.user_state
   

**What happens?**\ 

-  The data folder for the repository is created.
-  One sql file per table is created with the table name was the file
   name.
-  A data.json file is created in data folder as well.

**Notes:**\  If you are just setting up data pulls for the first time,
you can add one or more tables with the [-t [table name]] option.

-pushdata: Push data from repository to database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have your data in your repository, pushing data is easy.

.. code-block::

   -pushdata -repo [repository name] [db connection information]

e.g. For pushing by table(s).

.. code-block::

   -pushdata -dt error_set -dt process_state -repo mydb -d mylocaldb

e.g. For pushing all tables.

.. code-block::

   -pushdata -repo mydb -d mylocaldb

Output:

.. code-block::

   Pushing Data
   Running: error_set.sql

**Notes:**
For interdependent data pushes, create _pre_push.sql and _post_push.sql files in the
data folder to have pgvctrl execute before and after the data push.

e.g.

.. code-block::

    data/
        _pre_push.sql
        error_set.sql
        membership.user_state.sql
        _post_push.sql

-dump: Dump the repositories database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can dump the database based on the repository backing it. This means
includes/excludes for schemas and tables are honored during the database
backup.

.. code-block::

    -dump -repo [repository name] [db connection information]

e.g. For dumping the database.

.. code-block::

    -dump -repo mydb -d mylocaldb

Output:

.. code-block::

    Do you want to dump the database? [YES/NO]
    :[Type YES]
    Repository mydb database backed up


**What happens?**\

-  The _databaseBackup/[repository name] folder is created if it doesn't exist.
-  The backup [repository name][.environment].[string date] file is created.

-restore: Restore a repositories database from -dump
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can restore a repositories database based on a previous repository database dump.

.. code-block::

    -restore [repository name][.environment].[string date] -repo [repository name] [db connection information]

e.g. For dumping the database.

.. code-block::

    -restore mylocaldb.test.20190101 -repo mydb -d mylocaldb

Output:

.. code-block::

    Do you want to restore the database? [YES/NO]
    :[Type YES]
    Database mylocaldb.20190101 from repository mydb restored ['-d', 'mylocaldb'].


**What happens?**\

-  The _databaseBackup/[repository name]/[dump file] file is used to fill the empty database at [db connection information].

**Notes:**

#. Database for restore should an empty databases.


dbRepoConfig.json
~~~~~~~~~~~~~~~~~

The dbRepoConfig.json files is the configuration file for your
repositories. The defaultVersionStorage object is used to build the table
that stores your repository information in the database on
initialization. Each repository can be set up with different repository
table structures as you see fit. The root setting tells pgvctrl where to
look for the repositories.

.. code-block::

    {
        "defaultVersionStorage": {
            "env": "env",
            "isProduction": "is_production",
            "repository": "repository_name",
            "revision": "revision",
            "table": "repository_version",
            "tableOwner": null,
            "version": "version",
            "versionHash": "version_hash"
        },
        "dumpDatabaseOptionsDefault": "-Fc -Z4",
        "repositories": [
            {
                "dumpDatabaseOptions": "-Fc -Z4",
                "envs": {
                    "your_test": "1.0.1",
                    "your_qa": "1.0.0",
                    "your_prod": "0.9.0"
                },
                "name": "YouRepoName",
                "restoreDatabaseOptions": "-Fc -j 8",
                "versionStorage": {
                    "env": "env",
                    "isProduction": "is_production",
                    "repository": "repository_name",
                    "revision": "revision",
                    "table": "repository_version",
                    "tableOwner": null,
                    "version": "version",
                    "versionHash": "version_hash"
                }
            }
        ],
        "restoreDatabaseOptionsDefault": "-Fc -j 8",
        "root": "databases",
        "timeExecutions": false
    }

data.json
~~~~~~~~~

The data.json file holds the list of tables for pushing and pulling data
to and from your database. The column-inserts setting tells pgvctrl to
create the table with insert statements if set to true. If false, the
table is created with postgres copy. When data is pushed to the
database, all relationships are dropped and recreated when the copy is
complete.

Example data.json file:

.. code-block::

   [
       {
           "apply-order": 0,
           "column-inserts": true,
           "table": "error_set"
       },
       {
           "apply-order": 0,
           "column-inserts": true,
           "table": "membership.user_state"
       }
   ]

License
-------

This project is licensed under the MIT License, provided in repository.

Authors
-------

-  Heath Sutton - *Initial work* - `87th Street Development <https://github.com/87thstdev/pgvctrl/>`_.
