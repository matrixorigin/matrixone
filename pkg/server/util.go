package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"time"
)

func parseNullTermString(b []byte) (str []byte, remain []byte) {
	off := bytes.IndexByte(b, 0)
	if off == -1 {
		return nil, b
	}
	return b[:off], b[off+1:]
}

func parseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {
	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer: If the first byte of a packet is a length-encoded integer and its byte value is 0xfe, you must check the length of the packet to verify that it has enough space for a 8-byte integer.
	// TODO: 0xff is undefined

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func dumpLengthEncodedInt(buffer []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(buffer, byte(n))

	case n <= 0xffff:
		return append(buffer, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(buffer, 0xfd, byte(n), byte(n>>8), byte(n>>16))

	case n <= 0xffffffffffffffff:
		return append(buffer, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}

	return buffer
}

func parseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}

	return nil, false, n, io.EOF
}

func dumpLengthEncodedString(buffer []byte, bytes []byte) []byte {
	buffer = dumpLengthEncodedInt(buffer, uint64(len(bytes)))
	buffer = append(buffer, bytes...)
	return buffer
}

func dumpUint16(buffer []byte, n uint16) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	return buffer
}

func dumpUint32(buffer []byte, n uint32) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	return buffer
}

func dumpUint64(buffer []byte, n uint64) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	buffer = append(buffer, byte(n>>32))
	buffer = append(buffer, byte(n>>40))
	buffer = append(buffer, byte(n>>48))
	buffer = append(buffer, byte(n>>56))
	return buffer
}

func dumpBinaryTime(dur time.Duration) (data []byte) {
	if dur == 0 {
		return []byte{0}
	}
	data = make([]byte, 13)
	data[0] = 12
	if dur < 0 {
		data[1] = 1
		dur = -dur
	}
	days := dur / (24 * time.Hour)
	dur -= days * 24 * time.Hour
	data[2] = byte(days)
	hours := dur / time.Hour
	dur -= hours * time.Hour
	data[6] = byte(hours)
	minutes := dur / time.Minute
	dur -= minutes * time.Minute
	data[7] = byte(minutes)
	seconds := dur / time.Second
	dur -= seconds * time.Second
	data[8] = byte(seconds)
	if dur == 0 {
		data[0] = 8
		return data[:9]
	}
	binary.LittleEndian.PutUint32(data[9:13], uint32(dur/time.Microsecond))
	return
}

func lengthEncodedIntSize(n uint64) int {
	switch {
	case n <= 250:
		return 1

	case n <= 0xffff:
		return 3

	case n <= 0xffffff:
		return 4
	}

	return 9
}

const (
	expFormatBig     = 1e15
	expFormatSmall   = 1e-15
	defaultMySQLPrec = 5
)

func appendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	absVal := math.Abs(fVal)
	if absVal > math.MaxFloat64 || math.IsNaN(absVal) {
		return []byte{'0'}
	}
	isEFormat := false
	if bitSize == 32 {
		isEFormat = float32(absVal) >= expFormatBig || (float32(absVal) != 0 && float32(absVal) < expFormatSmall)
	} else {
		isEFormat = absVal >= expFormatBig || (absVal != 0 && absVal < expFormatSmall)
	}
	var out []byte
	if isEFormat {
		if bitSize == 32 {
			prec = defaultMySQLPrec
		}
		out = strconv.AppendFloat(in, fVal, 'e', prec, bitSize)
		valStr := out[len(in):]
		// remove the '+' from the string for compatibility.
		plusPos := bytes.IndexByte(valStr, '+')
		if plusPos > 0 {
			plusPosInOut := len(in) + plusPos
			out = append(out[:plusPosInOut], out[plusPosInOut+1:]...)
		}
		// remove extra '0'
		ePos := bytes.IndexByte(valStr, 'e')
		pointPos := bytes.IndexByte(valStr, '.')
		ePosInOut := len(in) + ePos
		pointPosInOut := len(in) + pointPos
		validPos := ePosInOut
		for i := ePosInOut - 1; i >= pointPosInOut; i-- {
			if out[i] == '0' || out[i] == '.' {
				validPos = i
			} else {
				break
			}
		}
		out = append(out[:validPos], out[ePosInOut:]...)
	} else {
		out = strconv.AppendFloat(in, fVal, 'f', prec, bitSize)
	}
	return out
}
