package tables

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/sqlgen"
)

type Conn interface {
	CopyTo(ctx context.Context, w io.Writer, sql string) (pgconn.CommandTag, error)
	Exec(ctx context.Context, sql string) *pgconn.MultiResultReader
}

func Copy(ctx context.Context, table string, def []sqlgen.ColDef, c Conn) ([][]string, error) {
	var err error
	// no position stored
	// copy the entire database
	b := &bytes.Buffer{}

	query := fmt.Sprintf(`COPY %s TO STDOUT WITH BINARY;`, table)
	log.Debug().Msg(query)

	_, err = c.CopyTo(ctx, b, query)
	if err != nil {
		log.Error().Err(err).Msg("copy error")
	}

	buf := buf(b.Bytes())
	buf.popBytes(11)

	// flags
	_ = buf.popInt32()

	// header extension
	_ = buf.popInt32()

	decs := decoders(def)
	if err != nil {
		return nil, fmt.Errorf("build decs: %w", err)
	}

	cols := [][]string{}

	for {
		if buf.peekNextByte(0xff, 2) {
			_ = buf.popBytes(2)
			break
		}

		row := []string{}

		nFields := buf.popInt16()

		if nFields != len(decs) {
			return nil, errors.New("wrong number of decoders for tuple fields")
		}

		for i := 0; i < nFields; i++ {
			if buf.peekNextByte(0xff, 4) {
				_ = buf.popBytes(4)

				row = append(row, "null")

				continue
			}

			fieldLen := buf.popInt32()
			row = append(row, decs[i].Decode(buf.popBytes(fieldLen)))
		}

		cols = append(cols, row)
	}

	return cols, nil
}
