package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zknill/sqledge/pkg/config"
	"github.com/zknill/sqledge/pkg/queryproxy"
	"github.com/zknill/sqledge/pkg/replicate"
)

type nameRow struct {
	id   int
	name string
}

const (
	dbName   = "test-db"
	userName = "postgres"
	password = "password"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func TestWritesOnPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	container := newDB(ctx, t)
	upstream := newSQLConn(ctx, t, container)
	cfg := defaultConfig(ctx, t, container)
	local := newSQLiteConn(ctx, t, cfg)

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancel := context.WithCancel(ctx)

	// start sqledge replication
	go func() {
		defer wg.Done()
		if err := replicate.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
			assert.NoError(t, err)
		}
	}()

	execStatements(
		t,
		upstream,
		"CREATE TABLE names (id serial not null primary key, name text);",
		"INSERT INTO names (name) VALUES ('hello'), ('world')",
	)

	// TODO: make this backoff retry
	<-time.After(2 * time.Second)

	got := readAllNameRows(t, local)

	want := []nameRow{
		{id: 1, name: "hello"},
		{id: 2, name: "world"},
	}

	assert.Equal(t, want, got)

	// cleanup
	cancel()
	wg.Wait()
}

func TestInitialCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	container := newDB(ctx, t)
	upstream := newSQLConn(ctx, t, container)
	cfg := defaultConfig(ctx, t, container)
	local := newSQLiteConn(ctx, t, cfg)

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancel := context.WithCancel(ctx)

	execStatements(
		t,
		upstream,
		"CREATE TABLE names (id serial not null primary key, name text);",
		"INSERT INTO names (name) VALUES ('hello'), ('world')",
	)

	// start sqledge replication
	go func() {
		defer wg.Done()
		if err := replicate.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
			assert.NoError(t, err)
		}
	}()

	// TODO: make this backoff retry
	<-time.After(2 * time.Second)

	got := readAllNameRows(t, local)

	want := []nameRow{
		{id: 1, name: "hello"},
		{id: 2, name: "world"},
	}

	assert.Equal(t, want, got)

	// cleanup
	cancel()
	wg.Wait()
}

func TestWriteForwarding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	container := newDB(ctx, t)
	upstream := newSQLConn(ctx, t, container)
	cfg := defaultConfig(ctx, t, container)
	local := newSQLiteConn(ctx, t, cfg)

	assert.NoError(t, upstream.Ping())

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancel := context.WithCancel(ctx)

	// start sqledge replication
	go func() {
		defer wg.Done()
		if err := replicate.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
			assert.NoError(t, err)
		}
	}()

	if err := queryproxy.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		assert.NoError(t, err)
	}

	<-time.After(1 * time.Second)
	t.Log("connecting to proxy")

	proxyConnStr := fmt.Sprintf(
		"user=postgres host=0.0.0.0 port=%d database=%s sslmode=disable",
		cfg.Proxy.Port,
		cfg.Upstream.DBName,
	)

	db, err := sql.Open("pgx", proxyConnStr)
	assert.NoError(t, err)

	t.Log("connected to proxy")

	execStatements(
		t,
		db,
		"CREATE TABLE names (id serial not null primary key, name text);",
		"INSERT INTO names (name) VALUES ('hello'), ('world')",
	)

	t.Log("executed statements")

	want := []nameRow{
		{id: 1, name: "hello"},
		{id: 2, name: "world"},
	}

	// assert rows are in upstream
	gotUpstream := readAllNameRows(t, upstream)
	assert.Equal(t, want, gotUpstream)

	// TODO: make this backoff retry

	// assert rows are in local
	got := readAllNameRows(t, local)
	assert.Equal(t, want, got)

	// cleanup
	cancel()
	wg.Wait()
}

func newDB(ctx context.Context, t *testing.T) *postgres.PostgresContainer {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
		postgres.WithDatabase(dbName),
		postgres.WithUsername(userName),
		postgres.WithPassword(password),
		postgres.WithConfigFile(defaultPostgresConfigFile(t)),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	return pgContainer
}

func newConn(ctx context.Context, t *testing.T, container *postgres.PostgresContainer) *pgconn.PgConn {
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	conn, err := pgconn.Connect(context.Background(), connStr)
	assert.NoError(t, err)

	return conn
}

func newSQLConn(ctx context.Context, t *testing.T, container *postgres.PostgresContainer) *sql.DB {
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	connStr = strings.ReplaceAll(connStr, "host=localhost", "host=0.0.0.0")

	db, err := sql.Open("pgx", connStr)
	assert.NoError(t, err)

	return db
}

func newSQLiteConn(ctx context.Context, t *testing.T, cfg *config.Config) *sql.DB {
	db, err := sql.Open("sqlite3", cfg.Local.Path)
	assert.NoError(t, err)

	return db
}

func defaultConfig(ctx context.Context, t *testing.T, container *postgres.PostgresContainer) *config.Config {
	cfg := config.Config{}

	port, _ := container.MappedPort(ctx, "5432/tcp")

	cfg.Upstream.User = userName
	cfg.Upstream.Pass = password
	cfg.Upstream.DBName = dbName
	cfg.Upstream.Schema = "public"
	cfg.Upstream.Address = "0.0.0.0"
	cfg.Upstream.Port = port.Int()

	cfg.Replication.Publication = "test_publication"
	cfg.Replication.Plugin = "pgoutput"
	cfg.Replication.SlotName = "sqledge_test_slot"
	cfg.Replication.CreateSlotIfNoExists = true
	cfg.Replication.Temporary = true

	f, err := os.CreateTemp(os.TempDir(), "sqledge-*.db")
	assert.NoError(t, err)

	cfg.Local.Path = f.Name()

	cfg.Proxy.Address = "localhost"
	cfg.Proxy.Port = rand.Intn(100) + 5433

	return &cfg
}

func defaultPostgresConfigFile(t *testing.T) string {
	content := `
listen_addresses = '*'
max_connections = 100			# (change requires restart)
shared_buffers = 128MB			# min 128kB
dynamic_shared_memory_type = posix	# the default is usually the first option
log_timezone = 'Etc/UTC'
datestyle = 'iso, mdy'
timezone = 'Etc/UTC'
lc_messages = 'en_US.utf8'			# locale for system error message
lc_monetary = 'en_US.utf8'			# locale for monetary formatting
lc_numeric = 'en_US.utf8'			# locale for number formatting
lc_time = 'en_US.utf8'				# locale for time formatting
default_text_search_config = 'pg_catalog.english'

wal_level = logical			# minimal, replica, or logical
max_wal_size = 1GB
min_wal_size = 80MB
max_wal_senders = 5		# max number of walsender processes
max_replication_slots = 5	# max number of replication slots
`

	f, err := os.CreateTemp(os.TempDir(), "postgres-config-*.conf")
	assert.NoError(t, err)

	f.WriteString(content)
	f.Close()

	return f.Name()
}

func execStatements(t *testing.T, db *sql.DB, statements ...string) {
	for _, stmt := range statements {
		_, err := db.Exec(stmt)
		assert.NoError(t, err)
		t.Log("executed: " + stmt)
	}
}

func readAllNameRows(t *testing.T, db *sql.DB) []nameRow {
	rows, err := db.Query(`SELECT * FROM names;`)
	assert.NoError(t, err)

	var out []nameRow

	for rows.Next() {
		n := nameRow{}

		assert.NoError(t, rows.Scan(&(n.id), &(n.name)))
		out = append(out, n)
	}

	return out
}
