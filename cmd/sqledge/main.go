package main

import (
	"context"
	"flag"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/config"
	"github.com/zknill/sqledge/pkg/queryproxy"
	"github.com/zknill/sqledge/pkg/replicate"
)

func main() {
	flag.Parse()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse config")
	}

	if err := queryproxy.Run(ctx, cfg); err != nil {
		log.Fatal().Err(err).Msg("failed to start sqledge")
	}

	if err := replicate.Run(ctx, cfg); err != nil {
		log.Fatal().Err(err).Msg("failed in replicate")
	}
}
