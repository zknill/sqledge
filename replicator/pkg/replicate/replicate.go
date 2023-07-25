package replicate

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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
		return nil, fmt.Errorf("new conn: %w")
	}

	return c, nil
}

func (c *Conn) Close() error {
	return c.Close()
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

func (c *Conn) Slot(slotName, outputPlugin string, createSlot, temporary bool) (*Slot, error) {
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

	return &Slot{
		conn: c.conn,
		args: pluginArguments,
		name: slotName,
		pos:  c.pos,
	}, nil
}

type Slot struct {
	conn *pgconn.PgConn

	args []string
	name string
	pos  pglogrepl.LSN

	msgs chan pglogrepl.Message
	errs chan error
	done chan struct{}
}

func (s *Slot) Start(ctx context.Context) error {
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

	return nil
}

func (s *Slot) Errors() <-chan error {
	return s.errs
}

func (s *Slot) Stream() <-chan pglogrepl.Message {
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	inStream := false

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
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

			select {
			case s.msgs <- logicalMsg:
			case <-s.done:
			}

			s.pos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (s *Slot) Close() error {
	close(s.done)
	return nil
}

func (s *Slot) sendErr(err error) {
	if err == nil {
		return
	}

	select {
	case s.errs <- err:
	case <-s.done:
	}
}

func (c *Conn) identify() error {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), c.conn)
	if err != nil {
		return fmt.Errorf("identify: %w", err)
	}

	c.pos = sysident.XLogPos
	return nil
}
