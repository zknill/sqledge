package main

import (
	"database/sql"
	"flag"
	"net"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/pgwire"
)

var (
	addr   = flag.String("address", "localhost:5433", "address to listen on")
	local  = flag.String("sqlite dsn", "./sqlite.db", "dsn for sqlite")
	remote = flag.String("remote", "localhost:5432", "address of remote database")
)

func main() {
	flag.Parse()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	localDB, err := sql.Open("sqlite3", "sqlite.db")
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer localDB.Close()

	remoteDB, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer remoteDB.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Error().Err(err).Msg("accept err")

			continue
		}

		pgwire.Handle(remoteDB, localDB, conn)
	}
}
