# SQLedge

[State: alpha]

SQLedge uses postgres logical replication to stream the changes in a source postgres database to an SQLite database that can run on the edge.

SQLedge is WIP, but will eventually intercept postgres connections, forward writes to the source database, and use the embedded SQLite database to serve reads.

This lets you run your apps on the edge, and have local (eventually consistent) access to your data.

## SQL generation

The `pkg/sqlgen` package has an SQL generator in it, which will generate sqlite insert, update, delete statements based on the logical replication messages received.

## SQL parsing

When the database is started, we look at which tables already exist in the sqlite copy, and make sure new tables are created automatically on the fly.

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
   SQLEDGE_DEMO_CONN_STRING="postgres://sqledger:secret@127.0.0.1/myappdatabase?replication=database" go run ./replicator/main.go
   ```

