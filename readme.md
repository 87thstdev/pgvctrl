# dbvctrl
Database **dbvctrl** is a tool designed to help deploy changes to postgres databases. All changes are stored in versioned folders with the order of execution set by the programmer.


## Prerequisites:
1. [postgres](https://www.postgresql.org/) ;)
1. A general knowledge of postgres sql. [tutorial](http://www.postgresqltutorial.com/)
1. Python3

## Getting started:

## pip install
<pre>pip install pgvctrl</pre>

## github
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

1. If you don't already have a database, create one on your postgres server.
1. Create pgvctrl dbRepoConfig:
    1. Make a directory where you want you database repositories to live.
    <pre>pgvctrl -mkconf</pre>
    This will create a dbRepoConfig.json file.
1. Create database repository:
    1. In the same directory as the dbRepoConfig.json file, run:
    <pre>pgvctrl -mkrepo [repository name]</pre>
    e.g
    <pre>pgvctrl -mkrepo mydb</pre>
    Output:
    <pre>Repository created: mydb</pre>

    __What just happened?__<br />
    * There will be a folder structure: [my dir]/databases/[repository name]/ created.
    * The dbRepoConfig.json file will be updated  to reflect the new repository. 
    
1. Initialize database repository:
    1. In the same directory as the dbRepoConfig.json file, run:
    <pre>pgvctrl -init [db connection information] -repo [repository name]</pre>
    For production databases:
    <pre>pgvctrl -init [db connection information] -repo [repository name] -production</pre>
    __NOTE:__<br />
    __Database connection information should include at a minimum.__
    
    _Standard Information_
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
    
    *Or*
    
    _Service Information [.pg_service](https://www.postgresql.org/docs/9.6/static/libpq-pgservice.html)_
    <pre>-svc [pg service information]</pre>
    e.g.
    <pre>pgvctrl -svc mydatabase:test -repo mydb</pre>
    
    __What just happened?__<br />
    After initialization is complete:
    * There will be a new table in your database named repository_version.
      This is where pbvctrl stores your repository name, version number with a
       version hash for each sql update file, environment name, revision (number of times
       the same version has been applied with different sql hash) and production flag.
       
1. Make repository version for repository: -mkv: Make version number:
    <pre>pgvctrl -mkv [x.x.x.version_name] -repo [repository name]</pre>
    e.g.:
    <pre>pgvctrl -mkv 1.0.0.my_new_version -repo mydb</pre>
    Output:
    <pre>Version mydb/1.0.0.my_new_version created.</pre>

1. Create sql change files in the versioned directory!  These files will be used to update your database and should
  have the naming convention of:<br />
  [order number].[change name].sql<br />
  e.g.: 100.AddedUserTable.sql

1. List repositories and changes:
    <pre>pgvctrl -rl</pre>
    Output:
    <pre>mydb
        v 1.0.0.my_new_version</pre>

    Verbose:
    <pre>pgvctrl -rlv</pre>
    Output:
    <pre>mydb
        v 0.0.0.my_new_version
            100 AddUsersTable</pre>

1. When you are ready to apply your changes to your database:
    <pre>pgvctrl -apply -v [version number] -repo [repository name] [db connection information]</pre>
    e.g.
    <pre>pgvctrl -apply -v 0.0.0 -repo mydb -d mylocaldb</pre>
    Output:
    <pre>Running: 100.AddUsersTable<br />...<br />Running: 500.AddStatesTable</pre>

    __*Notes:*__<br />
    * If you are applying changes to a production database, you must use the -production flag.

    __What just happened?__<br />
    * All of the sql files with [number].[change name].sql were ran against your database.
    * If you have "autoSnapshots" set to true, a snapshot was created in the _snapshots/[repository] directory
    * The repository_version table was update with the new version hash.

#### Working with environments:

Setting up environment versions in repositories help ensure versions get deployed to the proper
database.

#### Making and setting environments.

#### -mkenv: Make environment type:
<pre>pgvctrl -mkenv [env_name] -repo [repository name]</pre>
e.g.:
<pre>pgvctrl -mkenv test -repo mydb</pre>
Output:
<pre>Repository environment created: mydb test</pre>

#### -setenv: Set environment type to a version:
<pre>pgvctrl -setenv [env_name] -v [x.x] -repo [repository name]</pre>
e.g.:
<pre>pgvctrl -setenv test -v 1.0.0 -repo mydb</pre>
Output:
<pre>Repository environment set: mydb test 1.0.0</pre>

#### -init database with environment:
<pre>pgvctrl -init [db connection information] -repo [repository name] -setenv [env_name]</pre>
For production databases:
<pre>pgvctrl -init [db connection information] -repo [repository name] -setenv [env_name] -production</pre>
Output:
<pre>Database initialized environment [env_name]</pre>

#### -apply using -env:
<pre>pgvctrl -apply -env [env_name] -repo [repository name] [db connection information]</pre>
e.g.
<pre>pgvctrl -apply -env test -repo mydb -d mylocaldb</pre>
Output:
<pre>Running: 100.AddUsersTable<br />...<br />Running: 500.AddStatesTable<br />Applied: mydb v 1.1.0.MyVersion.0</pre>

## What else can pgvctrl do?
#### -chkver: Check the version and repo on a database:
<pre>pgvctrl -chkver -repo [repository name] [db connection information]</pre>
e.g:
<pre>pgvctrl -chkver -repo mydb -d mylocaldb</pre>
Output:
<pre>mydb: 0.0.0.first.0</pre>

#### -rmenv: Remove environment type:
<pre>pgvctrl -rmenv [env_name] -repo [repository name]</pre>
e.g.:
<pre>pgvctrl -rmenv test -repo mydb</pre>
Output:
<pre>Repository environment removed: mydb test</pre>

#### -rmrepo: Remove Repository
<pre>pgvctrl -rmrepo [repository name]</pre>
e.g.:
<pre>pgvctrl -rmrepo test</pre>
Output:
<pre>Repository removed: test</pre>
__*Notes:*__<br />
* If this command does not remove the folder from database, you must remove it and its contents yourself.  This is a safety measure.
* Any repository folders left behind will be displayed as UNREGISTERED when the -rl option is used.

### Fast Forward (-setff, -applyff)
__What are Fast Forwards?__<br />
Fast forwards are snapshots of the database structure at the time the snapshot was taken.

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
        "env": "env",
        "isProduction": "is_production",
        "repository": "repository_name",
        "revision": "revision",
        "table": "repository_version",
        "version": "version",
        "versionHash": "version_hash"
    },
    "repositories": [
        {
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
                "version": "version",
                "versionHash": "version_hash"
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