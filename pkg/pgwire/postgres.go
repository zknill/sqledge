package pgwire

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/sqlgen"
)

var withStatement = regexp.MustCompile(`with .* as (.*) select`)

func Handle(upstream, local *sql.DB, conn net.Conn) {
	if err := onStart(conn); err != nil {
		log.Error().Err(err).Msg("on start error")
	}

	log.Debug().Msg("completed startup")

	for {
		b := make([]byte, 5)

		if _, err := conn.Read(b); err != nil {
			log.Error().Err(err).Msg("read initial")
			return
		}

		if b[0] != SimpleQuery {
			log.Error().Msgf("unknown message type: %q", string(b[0]))
			return
		}

		l := binary.BigEndian.Uint32(b[1:5]) - 4

		body := make([]byte, l)

		if _, err := conn.Read(body); err != nil {
			log.Error().Err(err).Msg("read query body")
			return
		}

		query := strings.ToLower(string(body[:len(body)-1]))

		switch {
		case strings.HasPrefix(query, "select") || withStatement.MatchString(query):
			log.Debug().Msgf("querying: %q", string(query))

			rows, err := local.Query(query)
			if err != nil {
				log.Error().Err(err).Msg("local query")

				// TODO; how to return an error
				errResponse := pgproto3.ErrorResponse{Message: "query failed in proxy: " + err.Error()}
				out := errResponse.Encode(nil)

				ready := pgproto3.ReadyForQuery{TxStatus: 'I'}
				out = ready.Encode(out)

				conn.Write(out)

				continue
			}

			desc := rowDesc(rows)
			out := desc.Encode(nil)

			data := rowData(rows)
			for _, row := range data {
				out = row.Encode(out)
			}

			log.Debug().Msgf("found %d rows", len(data))

			cmd := &pgproto3.CommandComplete{CommandTag: []byte("")}
			out = cmd.Encode(out)

			ready := &pgproto3.ReadyForQuery{TxStatus: 'I'}
			out = ready.Encode(out)

			if _, err := conn.Write(out); err != nil {
				log.Error().Err(err).Msg("write response")

				continue
			}
		case strings.HasPrefix(query, "update"):
		case strings.HasPrefix(query, "insert"):
		case strings.HasPrefix(query, "delete"):
		default:
			// this should cover all unknown queries
			// update, delete, truncate, create table, etc.

		}

	}

}

// magic numbers come from here:
// - https://www.postgresql.org/docs/15/protocol-message-formats.html
const (
	SSLRequest     = 80877103
	StartupMessage = 196608

	AuthenticationOk = 'R'
	BackendKeyData   = 'K'
	ReadyForQuery    = 'Z'
	SimpleQuery      = 'Q'
)

// Eventually this method should parse the connection
// details and connect to the upstream database using them.
func onStart(conn net.Conn) error {
	readBuf := make([]byte, 4)

	if _, err := conn.Read(readBuf); err != nil {
		return fmt.Errorf("read msg len: %w", err)
	}

	l := binary.BigEndian.Uint32(readBuf) - 4

	if l < 4 || l > 10000 {
		return fmt.Errorf("invalid msg len: %d", l)
	}

	b := make([]byte, l)

	if _, err := conn.Read(b); err != nil {
		return fmt.Errorf("read msg: %w", err)
	}

	log.Debug().Msgf("startup message size: %d", l)

	msgType := binary.BigEndian.Uint32(b)

	switch msgType {
	case SSLRequest:
		conn.Write([]byte{'N'})
		return onStart(conn)

	case StartupMessage:
		// AuthenticationOk
		{
			success := uint32(0)
			l := uint32(8)
			d := make([]byte, l)

			binary.BigEndian.PutUint32(d[0:4], l)
			binary.BigEndian.PutUint32(d[4:], success)

			conn.Write([]byte{AuthenticationOk})
			conn.Write(d)

			log.Debug().Msgf("auth ok: len: %d, %s", l, string(AuthenticationOk))
		}

		// BackendKeyData
		{
			l := uint32(12)
			id := uint32(1234)
			secret := uint32(5678)

			d := make([]byte, l)
			binary.BigEndian.PutUint32(d[0:4], l)
			binary.BigEndian.PutUint32(d[4:8], id)
			binary.BigEndian.PutUint32(d[8:], secret)

			conn.Write([]byte{BackendKeyData})
			conn.Write(d)

			log.Debug().Msgf("backend key data: len: %d, id: %d, secret: %d, %s", l, id, secret, string(BackendKeyData))
		}

		// ReadyForQuery
		{
			l := uint32(5)
			d := make([]byte, l)

			binary.BigEndian.PutUint32(d[0:4], l)
			d[4] = 'I'

			conn.Write([]byte{ReadyForQuery})
			conn.Write(d)

			log.Debug().Msgf("ready for query: len: %d %s", l, string(ReadyForQuery))
		}
	}

	return nil
}

func rowData(rows *sql.Rows) []*pgproto3.DataRow {
	cols, err := rows.Columns()
	if err != nil {
		log.Error().Err(err).Msg("columns")

		return nil
	}

	data := []*pgproto3.DataRow{}

	for rows.Next() {
		row := make([][]byte, len(cols))
		dsts := make([]any, len(cols))

		for i := range row {
			dsts[i] = &row[i]
		}

		if err := rows.Scan(dsts...); err != nil {
			log.Error().Err(err).Msg("row scan")
			continue
		}

		data = append(data, &pgproto3.DataRow{Values: row})
	}

	return data
}

func rowDesc(rows *sql.Rows) *pgproto3.RowDescription {
	// TODO error handling
	types, err := rows.ColumnTypes()
	if err != nil {
		log.Error().Err(err).Msg("column types")

		return nil
	}

	cols, err := rows.Columns()
	if err != nil {
		log.Error().Err(err).Msg("columns")

		return nil
	}

	rowDesc := &pgproto3.RowDescription{}

	for i, c := range cols {
		oid, size := sqlgen.ColType(strings.ToLower(types[i].DatabaseTypeName())).PgType()

		rowDesc.Fields = append(rowDesc.Fields, pgproto3.FieldDescription{
			Name:         []byte(c),
			DataTypeOID:  uint32(oid),
			DataTypeSize: int16(size),
			TypeModifier: -1,
		})
	}

	return rowDesc
}
