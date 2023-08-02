package config

import (
	"fmt"

	"github.com/joeshaw/envdecode"
)

type Config struct {
	Upstream struct {
		User    string `env:"SQLEDGE_UPSTREAM_USER,default=postgres"`
		Pass    string `env:"SQLEDGE_UPSTREAM_PASSWORD"`
		Address string `env:"SQLEDGE_UPSTREAM_ADDRESS,default=localhost"`
		Port    int    `env:"SQLEDGE_UPSTREAM_PORT,default=5432"`
		DBName  string `env:"SQLEDGE_UPSTREAM_NAME,default=postgres"`
		Schema  string `env:"SQLEDGE_UPSTREAM_SCHEMA,default=public"`
	}

	Replication struct {
		Plugin               string `env:"SQLEDGE_REPLICATION_PLUGIN,default=pgoutput"`
		SlotName             string `env:"SQLEDGE_REPLICATION_SLOT_NAME,default=sqledge"`
		CreateSlotIfNoExists bool   `env:"SQLEDGE_REPLICATION_CREATE_SLOT,default=true"`
		Temporary            bool   `env:"SQLEDGE_REPLICATION_TEMP_SLOT,default=true"`
		Publication          string `env:"SQLEDGE_REPLICATION_PUBLICATION,default=sqledge"`
	}

	Local struct {
		Path string `env:"SQLEDGE_LOCAL_DB_PATH,default=./sqledge.db"`
	}

	Proxy struct {
		Address string `env:"SQLEDGE_PROXY_ADDRESS,default=localhost"`
		Port    int    `env:"SQLEDGE_PROXY_ADDRESS,default=5433"`
	}
}

func (c *Config) PostgresConnString() string {
	pass := ""
	if c.Upstream.Pass != "" {
		pass = ":" + c.Upstream.Pass
	}

	s := fmt.Sprintf("postgres://%s%s@%s:%d/%s?application_name=sqledge",
		c.Upstream.User,
		pass, c.Upstream.Address,
		c.Upstream.Port,
		c.Upstream.DBName,
	)

	return s
}

func Load() (*Config, error) {
	var c Config

	if err := envdecode.StrictDecode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &c, nil
}
