# SQLedge

SQLedge uses postgres logical replication to stream the changes in a source postgres database to an SQLite database that can run on the edge.

SQLedge is WIP, but will eventually intercept postgres connections, forward reads to the source database, and use the embedded SQLite database to serve reads. 

This lets you run your apps on the edge, and have local (eventually consistent) access to your data. 

## SQL generation

The `pkg/sqlgen` package has an SQL generator in it, which will generate sqlite insert, update, delete statements based on the logical replication messages received.

## SQL parsing

When the database is started, we look at which tables already exist in the sqlite copy, and make sure new tables are created automatically on the fly.
