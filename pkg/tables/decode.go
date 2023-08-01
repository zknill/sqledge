package tables

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/zknill/sqledge/pkg/sqlgen"
)

type FieldDecoder interface {
	Decode(b []byte) string
	numeric() bool
}

func decoders(def []sqlgen.ColDef) []FieldDecoder {
	out := make([]FieldDecoder, len(def))
	for i, d := range def {
		d := d

		switch d.Type {
		case sqlgen.PgColTypeText:
			out[i] = new(str)
		case sqlgen.PgColTypeInt2:
			out[i] = new(int2)
		case sqlgen.PgColTypeInt4:
			out[i] = new(int4)
		case sqlgen.PgColTypeInt8:
			out[i] = new(int8)
		case sqlgen.PgColTypeNum:
			out[i] = new(numeric)
		case sqlgen.PgColTypeFloat4:
			out[i] = new(float4)
		case sqlgen.PgColTypeFloat8:
			out[i] = new(float8)
		case sqlgen.PgColTypeBytea:
			out[i] = new(bytea)
		case sqlgen.PgColTypeJson:
			out[i] = new(str)
		case sqlgen.PgColTypeJsonB:
			out[i] = new(jsonb)
		case sqlgen.PgColTypeBool:
			out[i] = new(boolean)
		default:
			out[i] = new(str)
		}

		if d.Array {
			out[i] = &arr{elem: out[i]}
		}
	}

	return out
}

type int2 struct{}

func (i *int2) numeric() bool { return true }
func (i *int2) Decode(b []byte) string {
	v := binary.BigEndian.Uint16(b)
	return strconv.FormatUint(uint64(v), 10)
}

type int4 struct{}

func (i *int4) numeric() bool { return true }
func (i *int4) Decode(b []byte) string {
	v := binary.BigEndian.Uint32(b)
	return strconv.FormatUint(uint64(v), 10)
}

type int8 struct{}

func (i *int8) numeric() bool { return true }
func (i *int8) Decode(b []byte) string {
	v := binary.BigEndian.Uint64(b)
	return strconv.FormatUint(uint64(v), 10)
}

type float4 struct{}

func (f *float4) numeric() bool { return true }
func (f *float4) Decode(b []byte) string {
	v := binary.BigEndian.Uint32(b)
	return strconv.FormatFloat(float64(math.Float32frombits(v)), 'f', -1, 32)
}

type float8 struct{}

func (f *float8) numeric() bool { return true }
func (f *float8) Decode(b []byte) string {
	v := binary.BigEndian.Uint64(b)
	return strconv.FormatFloat(math.Float64frombits(v), 'f', -1, 64)
}

type numeric struct{}

func (n *numeric) numeric() bool { return true }
func (n *numeric) Decode(b []byte) string {
	buf := buf(b)
	ndigits := buf.popInt16()
	weight := buf.popInt16()
	// sign
	_ = buf.popInt16()
	dscale := buf.popInt16()

	s := &strings.Builder{}

	for i := 0; i < ndigits; i++ {
		x := buf.popInt16()
		if weight > 0 {
			x *= int(math.Pow10(weight))
			weight--
		}

		if i == ndigits-1 {
			x = withoutZeros(x)
		}

		fmt.Fprintf(s, "%d", x)
	}

	v, _ := strconv.ParseInt(s.String(), 10, 64)
	out := float64(v) / math.Pow10(dscale)

	return strconv.FormatFloat(out, 'f', -1, 64)
}

func withoutZeros(i int) int {
	result := 0
	mul := 1
	for remainingDigits := i; remainingDigits > 0; remainingDigits /= 10 {
		lastDigit := remainingDigits % 10
		if lastDigit != 0 {
			result += lastDigit * mul
			mul *= 10
		}
	}
	return result
}

type str struct{}

func (s *str) numeric() bool          { return false }
func (s *str) Decode(b []byte) string { return string(b) }

type jsonb struct{}

func (j *jsonb) numeric() bool          { return false }
func (j *jsonb) Decode(b []byte) string { return string(b[1:]) }

type bytea struct{}

func (b *bytea) numeric() bool          { return false }
func (b *bytea) Decode(v []byte) string { return string(v) }

type boolean struct{}

func (b *boolean) numeric() bool { return false }
func (b *boolean) Decode(v []byte) string {
	if v[0] == 0x01 {
		return "true"
	}

	return "false"
}

type arr struct {
	elem FieldDecoder
}

func (d *arr) numeric() bool { return d.elem.numeric() }

func (d *arr) Decode(b []byte) string {
	buf := buf(b)

	ndim := buf.popInt32()
	if ndim > 1 {
		panic("does not support multi dimension arrays")
	}

	hasNull := buf.popInt32()
	elemType := buf.popInt32()
	dim := buf.popInt32()
	lb := buf.popInt32()

	log.Trace().Msgf("num dimensions: %d", ndim)
	log.Trace().Msgf("hasNull: %d", hasNull)
	log.Trace().Msgf("elemType: %d", elemType)
	log.Trace().Msgf("dimensions: %d", dim)
	log.Trace().Msgf("lower bound: %d", lb)

	out := bytes.Buffer{}

	out.WriteRune('{')

	for i := 0; i < dim; i++ {
		if hasNull == 1 {
			if buf.peekNextByte(0xff, 4) {
				out.WriteString("null")

				if i < dim-1 {
					out.WriteString(", ")
				}

				_ = buf.popBytes(4)

				continue
			}

		}

		fieldLen := buf.popInt32()

		if !d.elem.numeric() {
			out.WriteString(`"`)
		}

		out.WriteString(d.elem.Decode(buf.popBytes(fieldLen)))

		if !d.elem.numeric() {
			out.WriteString(`"`)
		}

		if i < dim-1 {
			out.WriteString(", ")
		}
	}
	out.WriteRune('}')

	return out.String()
}
