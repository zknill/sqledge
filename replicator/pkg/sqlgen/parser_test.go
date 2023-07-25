package sqlgen_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zknill/sqledge/replicator/pkg/sqlgen"
)

func TestParseSql(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
		wantCols  []sqlgen.ColDef
	}{
		{

			name: "simple table",
			sql: `CREATE TABLE my_table (
            	    id TEXT PRIMARY KEY,
            	    value INTEGER
            	  );`,
			wantTable: "my_table",
			wantCols: []sqlgen.ColDef{
				{Name: "id", Type: "TEXT", PrimaryKey: true},
				{Name: "value", Type: "INTEGER"},
			},
		},
		{

			name: "composite primary key",
			sql: `CREATE TABLE my_table (
                    id TEXT,
                    value INTEGER,
                    rr REAL,
					other BLOB,
                    PRIMARY KEY (id, value)
                  );`,
			wantTable: "my_table",
			wantCols: []sqlgen.ColDef{
				{Name: "id", Type: "TEXT", PrimaryKey: true},
				{Name: "value", Type: "INTEGER", PrimaryKey: true},
				{Name: "rr", Type: "REAL", PrimaryKey: false},
				{Name: "other", Type: "BLOB", PrimaryKey: false},
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {

			p := sqlgen.NewParser(test.sql)

			table, cols, err := p.Parse()
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, test.wantTable, table)
			assert.Equal(t, test.wantCols, cols)
		})
	}

}
