package tables

import (
	"database/sql"
	"fmt"

	"github.com/zknill/sqledge/pkg/sqlgen"
)

type Querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func ColDefs(db Querier, table string) ([]sqlgen.ColDef, error) {
	query := `
	SELECT column_name column, udt_name as type
    FROM information_schema.columns 
	WHERE table_name = $1
	ORDER BY ordinal_position;
	`

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("query schema: %w", err)
	}

	var n, t string
	var arr bool

	defs := []sqlgen.ColDef{}

	for rows.Next() {
		arr = false

		if err := rows.Scan(&n, &t); err != nil {
			return nil, fmt.Errorf("scan")
		}

		if t[0] == '_' {
			t = t[1:]
			arr = true
		}

		defs = append(defs, sqlgen.ColDef{
			Name:       n,
			Type:       sqlgen.ColType(t),
			PrimaryKey: false,
			Array:      arr,
		})
	}

	return defs, nil
}

func TableColDefs(db Querier, schema string, filterTables []string) (map[string][]sqlgen.ColDef, error) {
	tables := make([]string, len(filterTables))
	copy(tables, filterTables)

	if len(tables) == 0 {
		t, err := findTables(db, schema)
		if err != nil {
			return nil, fmt.Errorf("find tables: %w", err)
		}

		tables = t
	}

	out := make(map[string][]sqlgen.ColDef)

	for _, t := range tables {
		defs, err := ColDefs(db, t)
		if err != nil {
			return nil, fmt.Errorf("col definitions for %q.%q: %w", schema, t, err)
		}

		out[t] = defs
	}

	return out, nil
}

func findTables(db Querier, schema string) ([]string, error) {
	query := `SELECT table_name 
	FROM information_schema.tables 
	WHERE table_schema = $1;`

	rows, err := db.Query(query, schema)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}

	var t string
	var out []string

	for rows.Next() {
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}

		out = append(out, t)
	}

	return out, nil
}
