package replicate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/sqlgen"
	"github.com/zknill/sqledge/pkg/tables"
)

type Conn struct {
	publication string
	conn        *pgconn.PgConn
	connStr     string

	pos pglogrepl.LSN
}

func NewConn(ctx context.Context, connString, publication string) (*Conn, error) {
	conn, err := pgconn.Connect(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("pgconnect: %w", err)
	}

	c := &Conn{
		publication: publication,
		conn:        conn,
		connStr:     connString,
	}

	if err := c.identify(); err != nil {
		return nil, fmt.Errorf("new conn: %w", err)
	}

	return c, nil
}

func (c *Conn) Close() error {
	return c.conn.Close(context.Background())
}

func (c *Conn) DropPublication() error {
	// TODO: care for injection
	result := c.conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", c.publication))

	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}

	return nil
}

func (c *Conn) CreatePublication() error {
	result := c.conn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", c.publication))

	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	return nil
}

type SlotConfig struct {
	SlotName             string
	OutputPlugin         string
	CreateSlotIfNoExists bool
	Temporary            bool
	Schema               string
}

type DBDriver interface {
	Pos() (string, error)
	Execute(query string) error
}

type SQLGen interface {
	Relation(*pglogrepl.RelationMessageV2) (string, error)
	Begin(*pglogrepl.BeginMessage) (string, error)
	Commit(*pglogrepl.CommitMessage) (string, error)
	Insert(*pglogrepl.InsertMessageV2) (string, error)
	Update(*pglogrepl.UpdateMessageV2) (string, error)
	Delete(*pglogrepl.DeleteMessageV2) (string, error)
	Truncate(*pglogrepl.TruncateMessageV2) (string, error)
	StreamStart(*pglogrepl.StreamStartMessageV2) (string, error)
	StreamStop(*pglogrepl.StreamStopMessageV2) (string, error)
	StreamCommit(*pglogrepl.StreamCommitMessageV2) (string, error)
	StreamAbort(*pglogrepl.StreamAbortMessageV2) (string, error)

	Pos(p string) string
	CopyCreateTable(schema, tableName string, colDefs []sqlgen.ColDef) (string, error)
	InsertCopyRow(schema, tableName string, colDefs []sqlgen.ColDef, rowValues []string) (string, error)
}

func (c *Conn) Stream(ctx context.Context, cfg SlotConfig, d DBDriver, gen SQLGen) error {
	pos, err := d.Pos()
	if err != nil {
		return fmt.Errorf("find starting pos: %w", err)
	}

	if pos != "" {
		lsn, err := pglogrepl.ParseLSN(pos)
		switch {
		case err == nil:
			c.pos = lsn
		case errors.Is(err, sql.ErrNoRows):
			// no op
		case err != nil:
			return fmt.Errorf("parse pos: %w", err)

		}
	}

	slot, err := c.slot(cfg.SlotName, cfg.OutputPlugin, cfg.CreateSlotIfNoExists, cfg.Temporary, c.pos)
	if err != nil {
		return fmt.Errorf("build slot: %w", err)
	}

	if pos == "" {
		log.Debug().Msg("starting copy")

		if err := c.initialCopy(ctx, cfg.Schema, slot.startSnapshot, d, gen); err != nil {
			return fmt.Errorf("copy: %w", err)
		}

		log.Debug().Msg("finished copy")

		if err := d.Execute(gen.Pos(c.pos.String())); err != nil {
			return fmt.Errorf("track position after copy: %w", err)
		}
	}

	log.Debug().Msgf("starting slot from pos: %q", c.pos)

	if err := slot.start(ctx); err != nil {
		return fmt.Errorf("start slot: %w", err)
	}

	var (
		logicalMsg pglogrepl.Message
		query      string
	)

	stream := slot.stream()

	for {
		select {
		case <-ctx.Done():
			slot.close()
			return ctx.Err()
		case <-slot.errs:
			return fmt.Errorf("slot error: %w", err)
		case logicalMsg = <-stream:
		}

		switch logicalMsg := logicalMsg.(type) {
		case *pglogrepl.RelationMessageV2:
			query, err = gen.Relation(logicalMsg)
		case *pglogrepl.BeginMessage:
			query, err = gen.Begin(logicalMsg)
		case *pglogrepl.CommitMessage:
			query, err = gen.Commit(logicalMsg)
		case *pglogrepl.InsertMessageV2:
			query, err = gen.Insert(logicalMsg)
		case *pglogrepl.UpdateMessageV2:
			query, err = gen.Update(logicalMsg)
		case *pglogrepl.DeleteMessageV2:
			query, err = gen.Delete(logicalMsg)
		case *pglogrepl.TruncateMessageV2:
			query, err = gen.Truncate(logicalMsg)
		case *pglogrepl.TypeMessageV2:
		case *pglogrepl.OriginMessage:
		case *pglogrepl.LogicalDecodingMessageV2:
			log.Debug().Msgf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)
		case *pglogrepl.StreamStartMessageV2:
			query, err = gen.StreamStart(logicalMsg)
		case *pglogrepl.StreamStopMessageV2:
			query, err = gen.StreamStop(logicalMsg)
		case *pglogrepl.StreamCommitMessageV2:
			query, err = gen.StreamCommit(logicalMsg)
		case *pglogrepl.StreamAbortMessageV2:
			query, err = gen.StreamAbort(logicalMsg)
		default:
			log.Debug().Msgf("Unknown message type in pgoutput stream: %T", logicalMsg)
			continue
		}

		log.Debug().Msg(query)

		if err != nil {
			return fmt.Errorf("generate sql: %w", err)
		}

		if err = d.Execute(query); err != nil {
			return fmt.Errorf("apply sql: %w", err)
		}
	}
}

func (c *Conn) slot(slotName, outputPlugin string, createSlot, temporary bool, pos pglogrepl.LSN) (*slot, error) {
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", c.publication),
		"messages 'true'",
		"streaming 'false'",
	}

	s := &slot{
		conn: c.conn,
		args: pluginArguments,
		name: slotName,
		pos:  c.pos,
	}

	// TODO: automatically work out if slot exists
	if createSlot {
		res, err := pglogrepl.CreateReplicationSlot(
			context.Background(),
			c.conn,
			slotName,
			outputPlugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: temporary},
		)
		if err != nil {
			return nil, fmt.Errorf("create slot: %w", err)
		}

		s.startSnapshot = res.SnapshotName
	}

	return s, nil
}

func (c *Conn) identify() error {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), c.conn)
	if err != nil {
		return fmt.Errorf("identify: %w", err)
	}

	c.pos = sysident.XLogPos
	return nil
}

func tableColDefs(connStr, schema string) (map[string][]sqlgen.ColDef, error) {
	db, err := sql.Open("pgx", strings.Replace(connStr, "replication=database", "", 1))
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	defs, err := tables.TableColDefs(db, schema, nil)
	if err != nil {
		return nil, fmt.Errorf("load col definitions: %w", err)
	}

	return defs, nil
}

func (c *Conn) initialCopy(ctx context.Context, schema, snapshotName string, dst DBDriver, gen SQLGen) (err error) {
	if schema == "" {
		return fmt.Errorf("cannot copy for empty schema")
	}

	defs, err := tableColDefs(c.connStr, schema)
	if err != nil {
		return fmt.Errorf("load col defs: %w", err)
	}

	copyConn, err := pgconn.Connect(context.Background(), c.connStr)
	if err != nil {
		return fmt.Errorf("pgconnect: %w", err)
	}

	query := `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;`
	if snapshotName != "" {
		query += fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", snapshotName)
	}

	log.Debug().Msg(query)

	copyConn.Exec(ctx, query).Close()

	defer func() {
		defer copyConn.Close(ctx)

		if e := recover(); e != nil {
			copyConn.Exec(ctx, `ROLLBACK;`).Close()
			err = fmt.Errorf("recover: %v", e)
			log.Debug().Msg("ROLLBACK")
		}

		if err != nil {
			copyConn.Exec(ctx, `ROLLBACK;`).Close()
			log.Debug().Msg("ROLLBACK")
		}

		copyConn.Exec(ctx, `COMMIT;`).Close()
		log.Debug().Msg("COMMIT")
	}()

	for table, columns := range defs {
		var (
			query string
			vals  [][]string
		)

		query, err = gen.CopyCreateTable(schema, table, columns)

		if err = dst.Execute(query); err != nil {
			return fmt.Errorf("execute inital copy: %w", err)
		}

		log.Debug().Msg(query)
		vals, err = tables.Copy(ctx, table, columns, copyConn)
		if err != nil {
			return fmt.Errorf("copy table: %w", err)

		}

		for _, row := range vals {
			query, err = gen.InsertCopyRow(schema, table, columns, row)
			if err != nil {
				return fmt.Errorf("generate sql: %w", err)
			}

			log.Debug().Msg(query)

			if err = dst.Execute(query); err != nil {
				return fmt.Errorf("execute inital copy: %w", err)
			}
		}
	}

	return nil
}

type slot struct {
	conn *pgconn.PgConn

	args          []string
	name          string
	pos           pglogrepl.LSN
	startSnapshot string

	msgs chan pglogrepl.Message
	errs chan error
	done chan struct{}
}

func (s *slot) start(ctx context.Context) error {
	if s.msgs != nil {
		// already started
		return nil
	}

	err := pglogrepl.StartReplication(
		ctx,
		s.conn,
		s.name,
		s.pos,
		pglogrepl.StartReplicationOptions{PluginArgs: s.args},
	)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	s.msgs = make(chan pglogrepl.Message)
	s.errs = make(chan error)
	s.done = make(chan struct{})

	go s.listen()

	return nil
}

func (s *slot) Errors() <-chan error {
	return s.errs
}

func (s *slot) stream() <-chan pglogrepl.Message {
	return s.msgs
}

func (s *slot) listen() {
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	inStream := false

	for {
		select {
		case <-s.done:
			return
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			log.Trace().Msg("status heartbeat")
			err := pglogrepl.SendStandbyStatusUpdate(
				context.Background(),
				s.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: s.pos},
			)
			if err != nil {
				go s.sendErr(err)
			}

			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)

		rawMsg, err := s.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			go s.sendErr(err)
		}

		if err, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			go s.sendErr(fmt.Errorf("postgres wal error: %v", err))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			go s.sendErr(fmt.Errorf("unexpected message: %w", err))
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				go s.sendErr(fmt.Errorf("keep alive parse failed: %w", err))
				continue
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			log.Trace().Msg("process logical replication")

			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				go s.sendErr(fmt.Errorf("parse xlog data failed: %w", err))
				continue
			}

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
			if err != nil {
				go s.sendErr(fmt.Errorf("parse logical replication message failed: %w", err))
				continue
			}

			if _, ok := logicalMsg.(*pglogrepl.StreamStartMessageV2); ok {
				inStream = true
			}

			if _, ok := logicalMsg.(*pglogrepl.StreamStopMessageV2); ok {
				inStream = false
			}

			log.Trace().Msg("sending logical message")

			select {
			case s.msgs <- logicalMsg:
			case <-s.done:
			}

			s.pos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (s *slot) close() error {
	close(s.done)
	return nil
}

func (s *slot) sendErr(err error) {
	if err == nil {
		return
	}

	var evt *zerolog.Event

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		evt = log.Warn()
	} else {
		evt = log.Error()
	}

	evt.Err(err).Msg("send error")

	select {
	case s.errs <- err:
	case <-s.done:
	}
}
