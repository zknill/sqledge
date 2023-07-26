package sqlgen

import (
	"database/sql"
	"fmt"
)

type SqliteDriver struct {
	db  *sql.DB
	cfg SqliteConfig
}

func NewSqliteDriver(cfg SqliteConfig, db *sql.DB) *SqliteDriver {
	return &SqliteDriver{
		cfg: cfg,
		db:  db,
	}
}

func (s *SqliteDriver) Execute(query string) error {
	_, err := s.db.Exec(query)
	return err
}

func (s *SqliteDriver) Pos() (string, error) {
	query := `SELECT pos 
    FROM postgres_pos 
	WHERE source_db = ? 
	AND plugin = ?
	AND publication = ?;`

	row := s.db.QueryRow(query, s.cfg.SourceDB, s.cfg.Plugin, s.cfg.Publication)

	if err := row.Err(); err != nil {
		return "", fmt.Errorf("query pos: %w", err)
	}

	var pos string

	if err := row.Scan(&pos); err != nil {
		return "", fmt.Errorf("read position: %w", err)
	}

	return pos, nil
}

func (s *SqliteDriver) InitPositionTable() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS postgres_pos (
		source_db text, 
		plugin text, 
		publication text, 
		pos text, 
		PRIMARY KEY (source_db, plugin, publication)
	)`)
	if err != nil {
		return fmt.Errorf("create lsn table: %w", err)
	}

	return nil
}

func (s *SqliteDriver) CurrentSchema() (map[string]map[string]ColDef, error) {
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
