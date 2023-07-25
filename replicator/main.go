package main

import (
	"context"
	"database/sql"
	"flag"
	"os"

	"github.com/jackc/pglogrepl"
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

	conn, err := replicate.NewConn(context.Background(), os.Getenv("SQLEDGE_DEMO_CONN_STRING"), *publication)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	defer conn.Close()

	if err := conn.DropPublication(); err != nil {
		log.Fatal().Err(err).Send()
	}

	if err := conn.CreatePublication(); err != nil {
		log.Fatal().Err(err).Send()
	}

	createSlot := true

	slot, err := conn.Slot("replicate_demo_temp", *outputPlugin, createSlot, *temporarySlot)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	go func() {
		for err := range slot.Errors() {
			log.Error().Err(err).Send()
		}
	}()

	db, err = sql.Open("sqlite3", "sqlite.db")
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	init := sqlgen.NewSqliteDBInit(db)

	if err := init.InitDB(); err != nil {
		log.Fatal().Msg(err.Error())
	}

	schema, err := init.CurrentSchema()
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	cfg := sqlgen.SqliteConfig{
		SourceDB:    "postgres",
		Plugin:      "pgoutput",
		Publication: *publication,
	}

	sqlite := sqlgen.NewSqlite(cfg, schema)
	if err != nil {
		log.Fatal().Msgf(err.Error())
	}

	for msg := range slot.Stream() {
		processV2(sqlite, msg)
	}
}

func processV2(sqlite *sqlgen.Sqlite, logicalMsg pglogrepl.Message) {
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		query, err := sqlite.Relation(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("sqlgen:", err)
		}

		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}

	case *pglogrepl.BeginMessage:
		query, err := sqlite.Begin(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("begin:", err)
		}
		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}
	case *pglogrepl.CommitMessage:
		query, _ := sqlite.Commit(logicalMsg)
		log.Debug().Msg(query)
		_, err := db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}

	case *pglogrepl.InsertMessageV2:
		query, err := sqlite.Insert(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("sqlgen:", err)
		}

		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}
	case *pglogrepl.UpdateMessageV2:
		query, err := sqlite.Update(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("sqlgen:", err)
		}

		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}
	case *pglogrepl.DeleteMessageV2:
		query, err := sqlite.Delete(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("sqlgen:", err)
		}

		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}
	case *pglogrepl.TruncateMessageV2:
		query, err := sqlite.Truncate(logicalMsg)
		if err != nil {
			log.Fatal().Msgf("sqlgen:", err)
		}

		log.Debug().Msg(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal().Msgf("exec:", err)
		}
	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		log.Debug().Msgf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		log.Debug().Msgf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		log.Debug().Msgf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		log.Debug().Msgf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Debug().Msgf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Debug().Msgf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}
