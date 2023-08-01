# SQLedge

[State: alpha]

SQLedge uses postgres logical replication to stream the changes in a source postgres database to an SQLite database that can run on the edge.
SQLedge serves reads from it's local sqlite database, and forwards writes to the upstream postgres server that it's replicating from.

This lets you run your apps on the edge, and have local, fast, and eventually consistent access to your data.

![SQLedge](https://github.com/zknill/sqledge/blob/main/etc/sqledge.png?raw=true)

## SQL generation

The `pkg/sqlgen` package has an SQL generator in it, which will generate sqlite insert, update, delete statements based on the logical replication messages received.

## SQL parsing

When the database is started, we look at which tables already exist in the sqlite copy, and make sure new tables are created automatically on the fly.

## Postgres wire proxy

SQLedge contains a postgres wire proxy, default on localhost:5433. This proxy uses the local sqlite database for reads, and forwards writes to the upstream postgres server.

## Copy on startup

SQLEdge maintains a table called `postgres_pos`, this tracks the LSN (log sequence number) of the received logical replication messages so it can pick up processing where it left
off.

If no LSN is found, SQLedge will start a postgres `COPY` of all tables in the `public` schema. Creating the appropriate SQLite tables, and inserting data.

## Trying it out

1. Create a database

   ```
   create database myappdatabase;
   ```

2. Create a user -- must be a super user because we create a publication on all tables

   ```
   create user sqledger with login superuser password 'secret';
   ```


3. Run the example

   ```
   SQLEDGE_UPSTREAM_USER=sqledger SQLEDGE_UPSTREAM_PASSWORD=secret SQLEDGE_UPSTREAM_NAME=myappdatabase go run ./cmd/sqledge/main.go
   ```

4. Connect to the postgres wire proxy

   ```
   psql -h localhost -p 5433
 
   $ CREATE TABLE my_table (id serial not null primary key, names text);
   $ INSERT INTO my_table (names) VALUES ('Jane'), ('John');

   $ SELECT * FROM my_table;
   ```
   The read will be served from the local database

5. Connect to the local sqlite db

   ```
   sqlite3 ./sqledge.db

   .schema
   ```

## Config

All config is read from environment variables. The full list is available in the struct tags on the fields in `pkg/config/config.go`
