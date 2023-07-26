package sqlgen

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type SqliteConfig struct {
	SourceDB    string
	Plugin      string
	Publication string
}

type Sqlite struct {
	typeMap   *pgtype.Map
	relations map[uint32]*pglogrepl.RelationMessageV2
	// map[table_name]map[column_name]column_type
	current map[string]map[string]ColDef

	cfg SqliteConfig

	// TODO: move these to the parent
	// tx  bool
	pos pglogrepl.LSN
}

func NewSqlite(cfg SqliteConfig, current map[string]map[string]ColDef) *Sqlite {
	s := &Sqlite{
		typeMap:   pgtype.NewMap(),
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		current:   current,
		cfg:       cfg,
	}

	return s
}

// SQLite data types:
// NULL.    The value is a NULL value.
// INTEGER. The value is a signed integer,
//          stored in 0, 1, 2, 3, 4, 6, or
//          8 bytes depending on the magnitude
//          of the value.
// REAL.    The value is a floating point value,
//          stored as an 8-byte IEEE floating
//          point number.
// TEXT.    The value is a text string, stored
//          using the database encoding (UTF-8,
//          UTF-16BE or UTF-16LE).
// BLOB.    The value is a blob of data, stored
//          exactly as it was input.

var mappedSqLiteTypes = map[string]string{
	"int2":    "integer",
	"int4":    "integer",
	"int8":    "integer",
	"numeric": "real",
	"float4":  "real",
	"float8":  "real",
	"bytea":   "blob",
}

func (s *Sqlite) Relation(msg *pglogrepl.RelationMessageV2) (string, error) {
	s.relations[msg.RelationID] = msg

	ccols, exists := s.current[msg.Namespace+"_"+msg.RelationName]
	if !exists {
		// CREATE TABLE
		// doesn't exist as current table
		currentCols := map[string]ColDef{}

		buf := &bytes.Buffer{}
		pk := []string{}

		for idx, col := range msg.Columns {
			dt, ok := s.typeMap.TypeForOID(col.DataType)
			if !ok {
				return "", errors.New("unknown type")
			}

			mappedType := "text"

			if mt, ok := mappedSqLiteTypes[dt.Name]; ok {
				mappedType = mt
			}

			cd := ColDef{
				Type: mappedType,
			}

			fmt.Printf("col: %v\n", *col)
			if col.Flags == 1 {
				pk = append(pk, col.Name)
				cd.PrimaryKey = true
			}

			fmt.Fprintf(buf, "%s %s", col.Name, mappedType)

			if idx < len(msg.Columns)-1 {
				buf.WriteString(", ")
			}

			currentCols[col.Name] = cd
		}

		var pks string

		if len(pk) != 0 {
			pks = ", PRIMARY KEY (" + strings.Join(pk, ", ") + ") "
		}

		s.current[msg.Namespace+"_"+msg.RelationName] = currentCols

		return fmt.Sprintf(
			"CREATE TABLE %s_%s (%s%s);",
			msg.Namespace,
			msg.RelationName,
			buf.String(),
			pks,
		), nil
	}

	// ALTER TABLE
	statements := []string{}

	colsCovered := map[string]ColDef{}

	for k, v := range ccols {
		v := v
		colsCovered[k] = v
	}

	for _, col := range msg.Columns {
		delete(colsCovered, col.Name)

		dt, ok := s.typeMap.TypeForOID(col.DataType)
		if !ok {
			return "", errors.New("unknown type")
		}

		mappedType := "text"

		if mt, ok := mappedSqLiteTypes[dt.Name]; ok {
			mappedType = mt
		}

		ccol, ok := ccols[col.Name]
		if !ok {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s_%s ADD COLUMN %s %s;", msg.Namespace, msg.RelationName, col.Name, mappedType))
			ccols[col.Name] = ColDef{
				Name: col.Name,
				Type: mappedType,
			}
			continue
		}

		pk := col.Flags == 1

		if ccol.PrimaryKey && !pk {
			// TODO: DROP PK
		}

		if pk && !ccol.PrimaryKey {
			// TODO: ADD PK
		}

		if ccol.Type != mappedType {
			// TODO: Change col type, not supported in SQLite
		}
	}

	for k, v := range colsCovered {
		if v.PrimaryKey {
			// dropping PK cols not supported in sqlite
			continue
		}

		statements = append(statements, fmt.Sprintf("ALTER TABLE %s_%s DROP COLUMN %s;", msg.Namespace, msg.RelationName, k))
	}

	return strings.Join(statements, " "), nil
}

// Insert represents a single row insert.
// Multiple VALUES (...) inserted at once
// would be multiple calls to this Insert method.
func (s *Sqlite) Insert(msg *pglogrepl.InsertMessageV2) (string, error) {
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return "", errors.New("unknown relation")
	}

	cols, err := s.parseColums(rel, msg.Tuple.Columns)
	if err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	cBuf := &bytes.Buffer{}
	vBuf := &bytes.Buffer{}

	for idx, col := range cols {
		cBuf.WriteString(col.name)
		fmt.Fprintf(vBuf, "%v", col.val())

		if idx < len(cols)-1 {
			cBuf.WriteString(", ")
			vBuf.WriteString(", ")
		}
	}

	return fmt.Sprintf(
		"INSERT INTO %s_%s (%s) VALUES (%s);",
		rel.Namespace,
		rel.RelationName,
		cBuf.String(),
		vBuf.String(),
	), nil
}

func (s *Sqlite) Update(msg *pglogrepl.UpdateMessageV2) (string, error) {
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return "", errors.New("unknown relation")
	}

	cols, err := s.parseColums(rel, msg.NewTuple.Columns)
	if err != nil {
		return "", fmt.Errorf("new: %w", err)
	}

	whereCols := cols

	if msg.OldTuple != nil {
		// what happens on delete col?
		whereCols, err = s.parseColums(rel, msg.OldTuple.Columns)
		if err != nil {
			return "", fmt.Errorf("old: %w", err)
		}
	}

	buf := &bytes.Buffer{}
	for _, col := range cols {
		if col.key && msg.OldTuple == nil {
			continue
		}

		fmt.Fprint(buf, col.kvSql(), ",")

	}

	kBuf := &bytes.Buffer{}

	for _, col := range whereCols {
		if !col.key {
			continue
		}

		fmt.Fprint(kBuf, col.kvSql(), " AND ")
	}

	return fmt.Sprintf(
		"UPDATE %s_%s SET %s WHERE %s;",
		rel.Namespace,
		rel.RelationName,
		buf.String()[:len(buf.String())-1],
		kBuf.String()[:len(kBuf.String())-5],
	), nil
}

func (s *Sqlite) Delete(msg *pglogrepl.DeleteMessageV2) (string, error) {
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return "", errors.New("unknown relation")
	}

	cols, err := s.parseColums(rel, msg.OldTuple.Columns)
	if err != nil {
		return "", fmt.Errorf("new: %w", err)
	}

	kBuf := &bytes.Buffer{}

	for _, col := range cols {
		if !col.key {
			continue
		}

		fmt.Fprint(kBuf, col.kvSql(), " AND ")
	}

	return fmt.Sprintf(
		"DELETE FROM %s_%s WHERE %s;",
		rel.Namespace,
		rel.RelationName,
		kBuf.String()[:len(kBuf.String())-5],
	), nil
}

func (s *Sqlite) Truncate(msg *pglogrepl.TruncateMessageV2) (string, error) {
	buf := &bytes.Buffer{}

	for _, id := range msg.RelationIDs {
		rel, ok := s.relations[id]
		if !ok {
			return "", errors.New("unknown relation")
		}

		fmt.Fprintf(buf, "DELETE FROM %s_%s; ", rel.Namespace, rel.RelationName)
	}

	return buf.String(), nil
}

func (s *Sqlite) Begin(msg *pglogrepl.BeginMessage) (string, error) {
	s.pos = msg.FinalLSN
	return "BEGIN TRANSACTION;", nil
}

func (s *Sqlite) StreamStart(msg *pglogrepl.StreamStartMessageV2) (string, error) {
	return "BEGIN TRANSACTION;", nil
}

func (s *Sqlite) StreamStop(msg *pglogrepl.StreamStopMessageV2) (string, error) {
	return "COMMIT;", nil
}

func (s *Sqlite) StreamCommit(msg *pglogrepl.StreamCommitMessageV2) (string, error) {
	return "COMMIT;", nil
}

func (s *Sqlite) StreamAbort(msg *pglogrepl.StreamAbortMessageV2) (string, error) {
	return "ROLLBACK;", nil
}

func (s *Sqlite) Commit(_ *pglogrepl.CommitMessage) (string, error) {
	return fmt.Sprintf(
		"INSERT OR REPLACE INTO postgres_pos (source_db, plugin, publication, pos) VALUES ('%s', '%s', '%s', '%s');\n COMMIT;",
		s.cfg.SourceDB, s.cfg.Plugin, s.cfg.Publication, s.pos,
	), nil
}

type column struct {
	name   string
	value  interface{}
	binary []byte
	key    bool
}

func (c *column) kvSql() string {
	return c.name + "=" + c.val()
}

func (c *column) val() string {
	if c.value == "null" {
		return "null"
	}

	if c.binary != nil {
		return fmt.Sprintf("x'%v'", c.binary)
	}

	return fmt.Sprintf("'%v'", c.value)
}

func (s *Sqlite) parseColums(rel *pglogrepl.RelationMessageV2, cols []*pglogrepl.TupleDataColumn) ([]*column, error) {
	out := make([]*column, len(cols))

	for idx, col := range cols {
		switch col.DataType {
		case 'n':
			out[idx] = &column{
				name:  rel.Columns[idx].Name,
				value: "null",
				key:   rel.Columns[idx].Flags == 1,
			}
		case 'u':
			// unchanged
		case 't':
			data := col.Data

			out[idx] = &column{
				name:  rel.Columns[idx].Name,
				value: string(data),
				key:   rel.Columns[idx].Flags == 1,
			}
		case 'b':
			out[idx] = &column{
				name:   rel.Columns[idx].Name,
				binary: col.Data,
				key:    rel.Columns[idx].Flags == 1,
			}
		}
	}

	return out, nil
}
