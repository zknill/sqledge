package queryproxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/config"
	"github.com/zknill/sqledge/pkg/pgwire"
)

func Run(ctx context.Context, cfg *config.Config) error {
	localDB, err := sql.Open("sqlite3", cfg.Local.Path)
	if err != nil {
		return fmt.Errorf("connect to local db: %w", err)
	}

	log.Debug().Msg("connected to local")

	remoteDB, err := sql.Open("pgx", cfg.PostgresConnString())
	if err != nil {
		return fmt.Errorf("connect to upstream db: %w", err)
	}

	log.Debug().Msgf("connected to remote %q, pinging", cfg.PostgresConnString())

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := remoteDB.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping upstream db: %w", err)
	}

	log.Debug().Msgf("connected to remote db, listening on %s:%d", cfg.Proxy.Address, cfg.Proxy.Port)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Proxy.Address, cfg.Proxy.Port))
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	go func() {
		defer remoteDB.Close()
		defer localDB.Close()
		defer lis.Close()
		<-ctx.Done()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

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
