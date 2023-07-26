package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/replicator/pkg/replicate"
	"github.com/zknill/sqledge/replicator/pkg/sqlgen"
)

var (
	outputPlugin  = flag.String("plugin", "pgoutput", "output plugin to request in replication slot")
	temporarySlot = flag.Bool("temporary", true, "create a temporary replication slot")
	publication   = flag.String("publication", "replicate_demo", "publication to listen to")
)

var db *sql.DB

func main() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	conn, err := replicateConnection(os.Getenv("SQLEDGE_DEMO_CONN_STRING"), *publication)
	if err != nil {
		log.Fatal().Err(err).Msg("create replicate connection")
	}
	defer conn.Close()

	db, err = sql.Open("sqlite3", "sqlite.db")
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	cfg := sqlgen.SqliteConfig{
		SourceDB:    "postgres",
		Plugin:      "pgoutput",
		Publication: *publication,
	}

	driver := sqlgen.NewSqliteDriver(cfg, db)

	if err := driver.InitPositionTable(); err != nil {
		log.Fatal().Msg(err.Error())
	}

	schema, err := driver.CurrentSchema()
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	sqlite := sqlgen.NewSqlite(cfg, schema)
	if err != nil {
		log.Fatal().Msgf(err.Error())
	}

	slot := replicate.SlotConfig{
		SlotName:             "sqledge_demo_temp",
		OutputPlugin:         *outputPlugin,
		CreateSlotIfNoExists: true,
		Temporary:            true,
	}

	if err := conn.Stream(
		context.Background(),
		slot,
		driver,
		sqlite,
	); err != nil {
		log.Fatal().Err(err).Msg("Streaming failed")
	}
}

func replicateConnection(connectionString, publication string) (*replicate.Conn, error) {
	conn, err := replicate.NewConn(context.Background(), connectionString, publication)
	if err != nil {
		return nil, fmt.Errorf("new conn: %w", err)
	}

	if err := conn.DropPublication(); err != nil {
		return nil, fmt.Errorf("drop publication: %w", err)
	}

	if err := conn.CreatePublication(); err != nil {
		return nil, fmt.Errorf("create publication: %w", err)
	}

	return conn, nil
}
