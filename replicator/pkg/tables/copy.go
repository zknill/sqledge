package tables

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/replicator/pkg/sqlgen"
)

type Conn interface {
	CopyTo(ctx context.Context, w io.Writer, sql string) (pgconn.CommandTag, error)
}

func Copy(ctx context.Context, table string, def []sqlgen.ColDef, c Conn) ([]string, error) {
	var err error
	// no position stored
	// copy the entire database
	b := &bytes.Buffer{}

	_, err = c.CopyTo(ctx, b, fmt.Sprintf(`copy %s to stdout with binary`, table))
	if err != nil {
		log.Error().Err(err).Msg("copy error")
	}

	buf := buf(b.Bytes())
	buf.popBytes(11)

	fmt.Println("flags", buf.popInt32())

	fmt.Println("header extension", buf.popInt32())

	decs := decoders(def)
	if err != nil {
		return nil, fmt.Errorf("build decs: %w", err)
	}

	cols := []string{}

	for {
		if buf.peekNextByte(0xff, 2) {
			_ = buf.popBytes(2)
			break
		}

		nFields := buf.popInt16()

		if nFields != len(decs) {
			return nil, errors.New("wrong number of decoders for tuple fields")
		}

		for i := 0; i < nFields; i++ {
			if buf.peekNextByte(0xff, 4) {
				_ = buf.popBytes(4)

				cols = append(cols, "null")

				continue
			}

			fieldLen := buf.popInt32()
			cols = append(cols, decs[i].Decode(buf.popBytes(fieldLen)))
		}
	}

	return cols, nil
}
