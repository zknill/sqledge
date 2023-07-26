package replicate

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
)

type Conn struct {
	publication string
	conn        *pgconn.PgConn

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
}

type FieldDecoder interface {
	Decode(b []byte) string
}

type int4 struct{}

func (i *int4) Decode(b []byte) string {
	v := binary.BigEndian.Uint32(b)
	return strconv.FormatUint(uint64(v), 10)
}

type str struct{}

func (s *str) Decode(b []byte) string {
	return string(b)
}

type dump struct {
	elem FieldDecoder
}

func (d *dump) Decode(b []byte) string {
	ndim := binary.BigEndian.Uint32(b[0:4])
	hasNull := binary.BigEndian.Uint32(b[4:8])
	elemType := binary.BigEndian.Uint32(b[8:12])
	dimensions := binary.BigEndian.Uint32(b[12:16])
	lbI := binary.BigEndian.Uint32(b[16:20])

	log.Trace().Msgf("num dimensions: %d", ndim)
	log.Trace().Msgf("hasNull: %d", hasNull)
	log.Trace().Msgf("elemType: %d", elemType)
	log.Trace().Msgf("dimensions: %d", dimensions)
	log.Trace().Msgf("lower bound: %d", lbI)

	low := uint32(20)
	high := uint32(24)

	buf := bytes.Buffer{}
	buf.WriteRune('{')

	for i := uint32(0); i < dimensions; i++ {
		if hasNull == 1 {
			if bytes.Equal(b[low:high], []byte{0xff, 0xff, 0xff, 0xff}) {
				buf.WriteString("null")
				if i < dimensions-1 {
					buf.WriteString(", ")
				}
				low = high
				high = low + 4
				continue
			}

		}

		fieldLen := binary.BigEndian.Uint32(b[low:high])

		low = high
		high = low + fieldLen

		buf.WriteString(d.elem.Decode(b[low:high]))
		if i < dimensions-1 {
			buf.WriteString(", ")
		}

		low = high
		high = low + 4
	}
	buf.WriteRune('}')

	return buf.String()
}

func (c *Conn) Stream(ctx context.Context, cfg SlotConfig, d DBDriver, gen SQLGen) error {
	pos, err := d.Pos()
	if err != nil {
		return fmt.Errorf("find starting pos: %w", err)
	}

	log.Debug().Msg("starting copy")

	// no position stored
	// copy the entire database
	b := &bytes.Buffer{}

	_, err = c.conn.CopyTo(ctx, b, `copy names to stdout with binary`)
	if err != nil {
		log.Error().Err(err).Msg("copy error")
	}

	low := uint32(11)
	high := low + 4

	//low, high = high, high+4

	fmt.Println("flags", binary.BigEndian.Uint32(b.Bytes()[low:high]))

	low = high
	high = low + 4

	headerExtension := binary.BigEndian.Uint32(b.Bytes()[low:high])

	low += headerExtension
	high += headerExtension

	fmt.Println("header extension", headerExtension)

	data := b.Bytes()

	decoders := []FieldDecoder{
		new(int4),
		new(str),
		new(str),
		&dump{elem: new(int4)},
		&dump{elem: new(str)},
	}

	for {
		low = high
		high = low + 2

		if bytes.Equal(data[low:high], []byte{0xff, 0xff}) {
			break
		}

		tupleFields := binary.BigEndian.Uint16(data[low:high])

		if tupleFields != uint16(len(decoders)) {
			return errors.New("wrong number of decoders for tuple fields")
		}

		for i := uint16(0); i < tupleFields; i++ {
			low = high
			high = low + 4

			if bytes.Equal(data[low:high], []byte{0xff, 0xff, 0xff, 0xff}) {
				fmt.Printf("NULL, ")
				continue
			}

			fieldLen := binary.BigEndian.Uint32(data[low:high])

			low = high
			high = low + fieldLen

			fmt.Printf(decoders[i].Decode(data[low:high]) + ", ")
		}
		fmt.Println()
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

	// TODO: automatically work out if slot exists
	if createSlot {
		_, err := pglogrepl.CreateReplicationSlot(
			context.Background(),
			c.conn,
			slotName,
			outputPlugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: temporary},
		)
		if err != nil {
			return nil, fmt.Errorf("create slot: %w", err)
		}
	}

	return &slot{
		conn: c.conn,
		args: pluginArguments,
		name: slotName,
		pos:  c.pos,
	}, nil
}

func (c *Conn) identify() error {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), c.conn)
	if err != nil {
		return fmt.Errorf("identify: %w", err)
	}

	c.pos = sysident.XLogPos
	return nil
}

type slot struct {
	conn *pgconn.PgConn

	args []string
	name string
	pos  pglogrepl.LSN

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
			go s.sendErr(fmt.Errorf("postgres wal error: %w", err))
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
	log.Error().Err(err).Msg("send error")
	if err == nil {
		return
	}

	select {
	case s.errs <- err:
	case <-s.done:
	}
}
