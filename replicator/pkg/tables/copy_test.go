package tables_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zknill/sqledge/replicator/pkg/sqlgen"
	"github.com/zknill/sqledge/replicator/pkg/tables"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func TestCopy(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
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

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	conn, err := pgconn.Connect(context.Background(), connStr)
	assert.NoError(t, err)

	def := []sqlgen.ColDef{
		{Type: sqlgen.PgColTypeInt2},
		{Type: sqlgen.PgColTypeInt4},
		{Type: sqlgen.PgColTypeInt8},
		{Type: sqlgen.PgColTypeText},
		{Type: sqlgen.PgColTypeText, Array: true},
		{Type: sqlgen.PgColTypeJson},
		{Type: sqlgen.PgColTypeJsonB},
		{Type: sqlgen.PgColTypeInt2, Array: true},
		{Type: sqlgen.PgColTypeInt4, Array: true},
		{Type: sqlgen.PgColTypeInt8, Array: true},
		{Type: sqlgen.PgColTypeText, Array: true},
		{Type: sqlgen.PgColTypeBool},
		{Type: sqlgen.PgColTypeBool, Array: true},
		{Type: sqlgen.PgColTypeNum},
		{Type: sqlgen.PgColTypeNum, Array: true},
		{Type: sqlgen.PgColTypeFloat4},
		{Type: sqlgen.PgColTypeFloat8},
		{Type: sqlgen.PgColTypeFloat4, Array: true},
		{Type: sqlgen.PgColTypeFloat8, Array: true},
		{Type: sqlgen.PgColTypeBytea},
		{Type: sqlgen.PgColTypeBytea, Array: true},
	}

	cols, err := tables.Copy(context.Background(), "alltypes", def, conn)
	assert.NoError(t, err)

	want := []string{
		"1",
		"2",
		"3",
		"a",
		"{'b'}",
		`"c"`,
		`"d"`,
		"{'4', '5'}",
		"{'6', '7'}",
		"{'9', '9'}",
		`{'e', 'f'}`,
		"true",
		"{'true', 'false', 'true'}",
		"10101.919191",
		"{'8888.111', '9999.222'}",
		"10.1",
		"11.2",
		"{'12.3', '12.4'}",
		"{'13.5', '13.6'}",
		"a",
		`{'b'}`,
	}

	assert.Equal(t, want, cols)

}
