package tables

import (
	"bytes"
	"encoding/binary"
)

type buf []byte

func (b *buf) popInt32() int {
	n := int(int32(binary.BigEndian.Uint32((*b)[:4])))
	*b = (*b)[4:]
	return n
}

func (b *buf) popInt16() (n int) {
	n = int(binary.BigEndian.Uint16((*b)[:2]))
	*b = (*b)[2:]
	return
}

func (b *buf) popStringN(high int) string {
	s := (*b)[:high]
	*b = (*b)[high:]
	return string(s)
}

func (b *buf) peekNextBytes(want []byte) bool {
	high := len(want)
	return bytes.Equal((*b)[:high], want)
}

func (b *buf) peekNextByte(next byte, n int) bool {
	high := n
	return bytes.Equal((*b)[:high], bytes.Repeat([]byte{next}, n))
}

func (b *buf) popBytes(high int) []byte {
	s := (*b)[:high]
	*b = (*b)[high:]
	return s
}
