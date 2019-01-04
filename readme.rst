dbvctrl
=======

Database **dbvctrl** is a tool designed to help deploy changes to
postgres databases. All changes are stored in versioned folders with the
order of execution set by the programmer.

Prerequisites:
--------------

1. `postgres <https://www.postgresql.org/>`__ ;)
2. A general knowledge of postgres sql.
   `tutorial <http://www.postgresqltutorial.com/>`__
3. Python3

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

1. If you don’t already have a database, create one on your postgres
   server.
2. Create pgvctrl dbRepoConfig:

   1. Make a directory where you want you database repositories to live.

      .. code-block::

         pgvctrl -mkconf

      This will create a dbRepoConfig.json file.

3. Create database repository:

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

4. Initialize database repository:

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

5. Make repository version for repository: -mkv: Make version number:

   .. code-block::

      pgvctrl -mkv [x.x.x.version_name] -repo [repository name]

   e.g.:

   .. code-block::

      pgvctrl -mkv 1.0.0.my_new_version -repo mydb

   Output:

   .. code-block::

      Version mydb/1.0.0.my_new_version created.

6. Create sql change files in the versioned directory! These files will
   be used to update your database and should have the naming convention
   of: [order number].[change name].sql e.g.: 100.AddedUserTable.sql

   **Notes:**\  \* For best results with sql files, wrap all statements
   in a Transactions.

   .. code-block::

       BEGIN TRANSACTION;
           [Your sql changes] 
       COMMIT;

7. List repositories and changes:

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

8. When you are ready to apply your changes to your database:

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

   **Notes:**\ 

   -  If you are applying changes to a production database, you must use
      the -production flag.

   **What just happened?**\ 

   -  All of the sql files with [number].[change name].sql were ran
      against your database.
   -  If you have “autoSnapshots” set to true, a snapshot was created in
      the \_snapshots/[repository] directory
   -  The repository_version table was update with the new version hash.

   #### SQL Error handling on -apply In the event of an SQL error,
   pgvctrl will attempt to run the rollback version of your sql.

   e.g

   .. code-block::


        100.AddUsers.sql
        100.AddUsers_rollback.sql - rollback file for 100.AddUsers.sql

   -  If your rollback file does not exist or fails, the -apply command
      fails and no sql after the first failing sql file will be ran.
   -  If the rollback file succeeds, all other sql files will be ran
      until all files have been applied if they can be.

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

**Notes:**\  \* If this command does not remove the folder from
database, you must remove it and its contents yourself. This is a safety
measure. \* Any repository folders left behind will be displayed as
UNREGISTERED when the -rl option is used.

Manage schemas and tables in Snapshots and Fast Forwards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Manage schemas (–schema, –exclude-schema, –rm-schema, –rmexclude-schema):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Allows the user to say what schemas structures to include/exclude
   when snapshots and Fast Forwards are created.
2. The ‘rm’ arguments allow the user to remove schemas from the included
   and excluded lists.

To include a schema:

.. code-block::

   pgvctrl --schema membership -repo pgvctrl_test

Output:

.. code-block::

   Repository added: pgvctrl_test
   include-schemas ['membership']

**What happens?**\ 

-  The dbRepoConfig.json file with have the membership schema added to
   the includeSchemas list property of the “pgvctrl_test” repository

Manage table (–table, –exclude-table, –rm-table, –rmexclude-table):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Allows the user to say what tables structures to include/exclude when
   snapshots and Fast Forwards are created.
2. The ‘rm’ arguments allow the user to remove tables from the included
   and excluded lists.

To include a table:

.. code-block::

   pgvctrl --table membership.user -repo pgvctrl_test

Output:

.. code-block::

   Repository added: pgvctrl_test
   include-table ['membership.user']

**Notes:** 1. If a table/schema is included and then later excluded, the
table/schema is moved from included to exclude and vice versa. 1.
Include table/schema works the same as with pg_dump.

Fast Forward (-setff, -applyff)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**What are Fast Forwards?**\  Fast forwards are snapshots of the
database structure at the time the snapshot was taken.

**Notes:** 1. There can be only one per repository version! 1.
Currently, only the schema is saved with fast forwards. 1. If there were
database schema changes outside of pgvctrl, it will be captured in the
fast forward. 1. Fast forwards should only be applied to empty
databases.

-setff: Set version fast forward
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -setff -repo [repository name] [db connection information]

-applyff: Apply version fast forward
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -applyff [Fast Forward Name] -repo [repository name] [db connection information]

Manage data (-pulldata, -pushdata)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There could be many reason why one would want to manage data: 1. Lookup
tables. 1. Testing data. 1. Just because your boss wants you too.

-pulldata: Pull data from repository by table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

   -pulldata [-t [table name]] -repo [repository name] [db connection information]

e.g.

.. code-block::

   -pulldata -t error_set -t membership.user_state -repo mydb -d mylocaldb

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

   -pushdata -t error_set -t process_state -repo mydb -d mylocaldb

e.g. For pushing all tables.

.. code-block::

   -pushdata -repo mydb -d mylocaldb

Output:

.. code-block::

   Pushing Data
   Running: error_set.sql

-dump-database: Dump the repositories database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can dump the database based on the repository backing it. This means
includes/excludes for schemas and tables are honored during the database
backup.

.. code-block::

   -dump-database -repo [repository name] [db connection information]

dbRepoConfig.json
~~~~~~~~~~~~~~~~~

The dbRepoConfig.json files is the configuration file for your
repositories. The autoSnapshots setting, if set to true, creates
snapshots of your repository each time a change is applied to your
database. The defaultVersionStorage object is used to build the table
that stores your repository information in the database on
initialization. Each repository can be set up with different repository
table structures as you see fit. The root setting tells pgvctrl where to
look for the repositories.

.. code-block::

    {
        "autoSnapshots": true,
        "dumpDatabaseOptionsDefault": "-Fc -Z 9",
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
        "repositories": [
            {
            "dumpDatabaseOptions": "-Fc -Z 9",
            "envs": {
                "your_test": "1.0.1",
                "your_qa": "1.0.0",
                "your_prod": "0.9.0"
            },
            "name": "YouRepoName",
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
        "root": "databases"
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
           "column-inserts": true,
           "table": "error_set"
       },
       {
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
