package queryproxy

import (
	"database/sql"
	"fmt"
	"net"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/config"
	"github.com/zknill/sqledge/pkg/pgwire"
)

func Run(cfg *config.Config) error {
	localDB, err := sql.Open("sqlite3", cfg.Local.Path)
	if err != nil {
		return fmt.Errorf("connect to local db: %w", err)
	}

	remoteDB, err := sql.Open("pgx", cfg.PostgresConnString())
	if err != nil {
		return fmt.Errorf("connect to upstream db: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.Proxy.ListenAddress)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	go func() {
		defer remoteDB.Close()
		defer localDB.Close()
		defer lis.Close()

		for {
			conn, err := lis.Accept()
			if err != nil {
				log.Error().Err(err).Msg("accept err")

				continue
			}

			pgwire.Handle(cfg.Upstream.Schema, remoteDB, localDB, conn)
		}
	}()

	return nil
}
