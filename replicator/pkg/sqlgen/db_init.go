package sqlgen

import (
	"database/sql"
	"fmt"
)

type SqliteDBInit struct {
	db *sql.DB
}

func NewSqliteDBInit(db *sql.DB) *SqliteDBInit {
	return &SqliteDBInit{
		db: db,
	}
}

func (s *SqliteDBInit) InitDB() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS postgres_lsn (
		source_db text, 
		plugin text, 
		publication text, 
		lsn text, 
		PRIMARY KEY (source_db, plugin, publication)
	)`)
	if err != nil {
		return fmt.Errorf("create lsn table: %w", err)
	}

	return nil
}

func (s *SqliteDBInit) CurrentSchema() (map[string]map[string]ColDef, error) {
	// tableName -> colName -> colDef
	out := make(map[string]map[string]ColDef)

	query := `SELECT tbl_name, sql FROM sqlite_schema WHERE type = 'table';`

	type tableRow struct {
		TableName string `db:"tbl_name"`
		SQL       string `db:"sql"`
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query schema: %w", err)
	}

	for rows.Next() {
		tr := tableRow{}
		if err := rows.Scan(&tr.TableName, &tr.SQL); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		tableName, cols, err := NewParser(tr.SQL).Parse()
		if err != nil {
			return nil, fmt.Errorf("parse table %q: %w", tr.TableName, err)
		}

		current := map[string]ColDef{}

		for i := range cols {
			col := cols[i]
			current[col.Name] = col
		}

		out[tableName] = current
	}

	return out, nil
}
