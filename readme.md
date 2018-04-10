# dbvctrl
Database **dbvctrl** is a tool designed to help deploy changes to postgres databases. All changes are stored in versioned folders with the order of execution set by the programmer.


## Prerequisites:
1. [postgres](https://www.postgresql.org/) ;)
1. A general knowledge of postgres sql. [tutorial](http://www.postgresqltutorial.com/)
1. Python3

## Getting started:

Download or clone the repository

## Install
<pre>python setup.py install</pre>
Get help:
<pre>pgvctrl -h</pre>
Get version:
<pre>pgvctrl -version</pre>

## Running the tests
In the test directory:
<pre>pytest</pre>

## Getting Started

1. If you don't already have a database, create one on your postges server.
    1. psql may create a table `test1`, not sure why, but it may.
1. Create pgvctrl dbRepoConfig:
    1. Make a directory where you want you database repositories to live.
    <pre>pgvctrl -mkconf</pre>
    This will create a dbRepoConfig.json file.
1. Initialize database repository:
    1. In the same directory as the dbRepoConfig.json file, run:
    <pre>pgvctrl -init [db connection information] -repo [repository name]</pre>
    For production databases:
    <pre>pgvctrl -init [db connection information] -repo [repository name] -production</pre>
    __NOTE:__<br />
    __Database connection information should include at a minimum.__
    <pre>-d [database name on server]</pre>
    e.g.
    <pre>pgvctrl -init -d mylocaldb -repo mydb</pre>
    Other information as needed:
    <pre>
    -host [postgres server host]
    -p [port]
    -u [database username]
    -pwd [password]
    </pre>

    __What just happened?__<br />
    After initialization is complete:
    * There will be a folder structure: [my dir]/databases/[repository name]/0.0/ created.
    This will be the location where you should but your sql update files.
    * There will be a new table in your database named repository_version.
      This is where pbvctrl stores your repository name and version number with a
       version hash for each sql update file.

1. Create sql change files in the versioned directory!  These files will be used to update your database and should
  have the naming convention of:<br />
  [order number].[change name].sql<br />
  e.g.: 100.AddedUserTable.sql

1. List repositories and changes:
    <pre>pgvctrl -repolist</pre>
    Output:
    <pre>mydb</pre>

    Verbose:
    <pre>pgvctrl -repolist -verbose</pre>
    Output:
    <pre>mydb
        v 0.0
            100 AddUsersTable</pre>

1. When you are ready to apply your changes to your database:
    <pre>pgvctrl -apply -v [version number] -repo [repository name] [db connection information]</pre>
    e.g.
    <pre>pgvctrl -apply -v 0.0 -repo mydb -d mylocaldb</pre>
    Output:
    <pre>Running: 100.AddUsersTable<br />...<br />Running: 500.AddStatesTable</pre>

    __*Notes:*__<br />
    * If you are applying changes to a production database, you must use the -production flag.

    __What just happened?__<br />
    * All of the sql files with [number].[change name].sql were ran against your database.
    * If you have "autoSnapshots" set to true, a snapshot was created in the _snapshots/[repository] directory
    * The repository_version table was update with the new version hash.


## What else can pgvctrl do?
#### -mkv: Make version number:
<pre>pgvctrl -mkv [x.x.version_name] -repo [repository name]</pre>
e.g.:
<pre>pgvctrl -mkv 1.0.my_new_version -repo mydb</pre>
Output:
<pre>Version mydb/1.0.my_new_version created.</pre>

#### -chkver: Check the version and repo on a database:
<pre>pgvctrl -chkver -repo [repository name] [db connection information]</pre>
e.g:
<pre>pgvctrl -chkver -repo mydb -d mylocaldb</pre>
Output:
<pre>mydb: 0.0</pre>

### Fast Forward (-setff, -applyff)
__What are Fast Forwards?__<br />
Fast forwards are snapshots of the database based on the repository and version it has.

__*Notes:*__
1. There can be only one per repository version!
1. Currently, only the schema is saved with fast forwards.
1. If there were database schema changes outside of pgvctrl, it will be captured in the fast forward.
1. Fast forwards should only be applied to empty databases. 

#### -setff: Set version fast forward
<pre>-setff -repo [repository name] [db connection information]</pre>

#### -applyff: Apply version fast forward
<pre>-applyff [Fast Forward Name] -repo [repository name] [db connection information]</pre>


### Manage data (-pulldata, -pushdata)
There could be many reason why one would want to manage data:
1. Lookup tables.
1. Testing data.
1. Just because your boss wants you too.


#### -pulldata: Pull data from repository by table
<pre>-pulldata [-t [table name]] -repo [repository name] [db connection information]</pre>
e.g.
<pre>-pulldata -t error_set -t membership.user_state -repo mydb -d mylocaldb</pre>
Output:
<pre>
Pulling: error_set
Pulling: membership.user_state
</pre>


__*What happens?*__<br />

* The data folder for the repository is created.
* One sql file per table is created with the table name was the file name.
* A data.json file is created in data folder as well.

__*Notes:*__<br />
If you are just setting up data pulls for the first time, you can add
one or more tables with the [-t [table name]] option.

#### -pushdata: Push data from repository to database
Once you have your data in your repository, pushing data is easy.
<pre>-pushdata -repo [repository name] [db connection information]</pre>
e.g. For pushing by table(s).
<pre>-pushdata -t error_set -repo mydb -d mylocaldb</pre>
e.g. For pushing all tables.
<pre>-pushdata -repo mydb -d mylocaldb</pre>


### dbRepoConfig.json
The dbRepoConfig.json files is the configuration file for
your repositories.  The autoSnapshots setting, if set to true, creates snapshots
of your repository each time a change is applied to your database.
The defaultVersionStorage object is used to build the table that stores your
repository information in the database on initialization. Each repository can be
set up with different repository table structures as you see fit.  The root setting
tells pgvctrl where to look for the repositories.
<pre>
{
    "autoSnapshots": true,
    "defaultVersionStorage": {
        "repository": "repository_name",
        "table": "repository_version",
        "version": "version",
        "versionHash": "version_hash"
    },
    "repositories": [
        {
            "name": "",
            "versionStorage": {
                "repository": "",
                "table": "",
                "version": "",
                "versionHash": ""
            }
        }
    ],
    "root": "databases"
}
</pre>

### data.json
The data.json file holds the list of tables for pushing and pulling
data to and from your database.  The column-inserts setting tells pgvctrl
to create the table with insert statements if set to true.  If false, the
table is created with postgres copy.  When data is pushed to the database,
all relationships are dropped and recreated when the copy is complete.

Example data.json file:
<pre>
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
</pre>

## License
This project is licensed under the MIT License, provided in repository.

## Authors
* Heath Sutton - *Initial work* - [87th Street Development]()