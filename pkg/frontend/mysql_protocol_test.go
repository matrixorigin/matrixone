package frontend

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/fagongzi/goetty"
	"math"
	"math/rand"
	"matrixone/pkg/config"
	"matrixone/pkg/defines"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/host"
	"strconv"
	"sync"
	"testing"
)

type TestRoutine struct {
	//io data
	io goetty.IOSession
	// whether the handshake succeeded
	established bool

	//mutex
	lock sync.Mutex

	ioPackage IOPackage

	//random bytes
	salt []byte

	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId uint8

	//max packet size of the client
	maxClientPacketSize uint32

	//the user of the client
	username string

	capacity string

	//joint capability shared by the server and the client
	capability uint32

	//collation id
	collationID int

	//collation name
	collationName string

	//character set
	charset string

	//the default database for the client
	database string

	//the id of the connection
	connectionID uint32

}

//read an int with length encoded from the buffer at the position
//return the int ; position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (tR *TestRoutine) readIntLenEnc(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	switch data[pos] {
	case 0xfb:
		//zero, one byte
		return 0, pos + 1, true
	case 0xfc:
		// int in two bytes
		if pos+2 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8
		return value, pos + 3, true
	case 0xfd:
		// int in three bytes
		if pos+3 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16
		return value, pos + 4, true
	case 0xfe:
		// int in eight bytes
		if pos+8 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 |
			uint64(data[pos+5])<<32 |
			uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 |
			uint64(data[pos+8])<<56
		return value, pos + 9, true
	}
	// 0-250
	return uint64(data[pos]), pos + 1, true
}

//write an int with length encoded into the buffer at the position
//return position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (tR *TestRoutine) writeIntLenEnc(data []byte, pos int, value uint64) int {
	switch {
	case value < 251:
		data[pos] = byte(value)
		return pos + 1
	case value < (1 << 16):
		data[pos] = 0xfc
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		return pos + 3
	case value < (1 << 24):
		data[pos] = 0xfd
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		data[pos+4] = byte(value >> 24)
		data[pos+5] = byte(value >> 32)
		data[pos+6] = byte(value >> 40)
		data[pos+7] = byte(value >> 48)
		data[pos+8] = byte(value >> 56)
		return pos + 9
	}
}

//append an int with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendIntLenEnc(data []byte, value uint64) []byte {
	tmp := make([]byte, 9)
	pos := tR.writeIntLenEnc(tmp, 0, value)
	return append(data, tmp[:pos]...)
}

//read the count of bytes from the buffer at the position
//return bytes slice ; position + count ; true - succeeded or false - failed
func (tR *TestRoutine) readCountOfBytes(data []byte, pos int, count int) ([]byte, int, bool) {
	if pos+count-1 >= len(data) {
		return nil, 0, false
	}
	return data[pos : pos+count], pos + count, true
}

//write the count of bytes into the buffer at the position
//return position + the number of bytes
func (tR *TestRoutine) writeCountOfBytes(data []byte, pos int, value []byte) int {
	pos += copy(data[pos:], value)
	return pos
}

//append the count of bytes to the buffer
//return the buffer
func (tR *TestRoutine) appendCountOfBytes(data []byte, value []byte) []byte {
	return append(data, value...)
}

//read a string with fixed length from the buffer at the position
//return string ; position + length ; true - succeeded or false - failed
func (tR *TestRoutine) readStringFix(data []byte, pos int, length int) (string, int, bool) {
	var sdata []byte
	var ok bool
	sdata, pos, ok = tR.readCountOfBytes(data, pos, length)
	if !ok {
		return "", 0, false
	}
	return string(sdata), pos, true
}

//write a string with fixed length into the buffer at the position
//return pos + string.length
func (tR *TestRoutine) writeStringFix(data []byte, pos int, value string, length int) int {
	pos += copy(data[pos:], value[0:length])
	return pos
}

//append a string with fixed length to the buffer
//return the buffer
func (tR *TestRoutine) appendStringFix(data []byte, value string, length int) []byte {
	return append(data, []byte(value[:length])...)
}

//read a string appended with zero from the buffer at the position
//return string ; position + length of the string + 1; true - succeeded or false - failed
func (tR *TestRoutine) readStringNUL(data []byte, pos int) (string, int, bool) {
	zeroPos := bytes.IndexByte(data[pos:], 0)
	if zeroPos == -1 {
		return "", 0, false
	}
	return string(data[pos : pos+zeroPos]), pos + zeroPos + 1, true
}

//write a string into the buffer at the position, then appended with 0
//return pos + string.length + 1
func (tR *TestRoutine) writeStringNUL(data []byte, pos int, value string) int {
	pos = tR.writeStringFix(data, pos, value, len(value))
	data[pos] = 0
	return pos + 1
}

//read a string with length encoded from the buffer at the position
//return string ; position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string; true - succeeded or false - failed
func (tR *TestRoutine) readStringLenEnc(data []byte, pos int) (string, int, bool) {
	var value uint64
	var ok bool
	value, pos, ok = tR.readIntLenEnc(data, pos)
	if !ok {
		return "", 0, false
	}
	sLength := int(value)
	if pos+sLength-1 >= len(data) {
		return "", 0, false
	}
	return string(data[pos : pos+sLength]), pos + sLength, true
}

//write a string with length encoded into the buffer at the position
//return position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string;
func (tR *TestRoutine) writeStringLenEnc(data []byte, pos int, value string) int {
	pos = tR.writeIntLenEnc(data, pos, uint64(len(value)))
	return tR.writeStringFix(data, pos, value, len(value))
}

//append a string with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendStringLenEnc(data []byte, value string) []byte {
	data = tR.appendIntLenEnc(data, uint64(len(value)))
	return tR.appendStringFix(data, value, len(value))
}

//append bytes with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendCountOfBytesLenEnc(data []byte, value []byte) []byte {
	data = tR.appendIntLenEnc(data, uint64(len(value)))
	return tR.appendCountOfBytes(data, value)
}

//append an int64 value converted to string with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendStringLenEncOfInt64(data []byte, value int64) []byte {
	var tmp []byte
	tmp = strconv.AppendInt(tmp, value, 10)
	return tR.appendCountOfBytesLenEnc(data, tmp)
}

//append an uint64 value converted to string with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendStringLenEncOfUint64(data []byte, value uint64) []byte {
	var tmp []byte
	tmp = strconv.AppendUint(tmp, value, 10)
	return tR.appendCountOfBytesLenEnc(data, tmp)
}

//append an float32 value converted to string with length encoded to the buffer
//return the buffer
func (tR *TestRoutine) appendStringLenEncOfFloat64(data []byte, value float64, bitSize int) []byte {
	var tmp []byte
	tmp = strconv.AppendFloat(tmp, value, 'f', -1, bitSize)
	return tR.appendCountOfBytesLenEnc(data, tmp)
}

//write the count of zeros into the buffer at the position
//return pos + count
func (tR *TestRoutine) writeZeros(data []byte, pos int, count int) int {
	for i := 0; i < count; i++ {
		data[pos+i] = 0
	}
	return pos + count
}

//the server calculates the hash value of the password with the algorithm
//and judges it with the authentication data from the client.
//Algorithm: SHA1( password ) XOR SHA1( slat + SHA1( SHA1( password ) ) )
func (tR *TestRoutine) checkPassword(password, salt, auth []byte) bool {
	if len(password) == 0 {
		return false
	}
	//hash1 = SHA1(password)
	sha := sha1.New()
	_, err := sha.Write(password)
	if err != nil {
		logutil.Errorf("SHA1(password) failed.")
		return false
	}
	hash1 := sha.Sum(nil)

	//hash2 = SHA1(SHA1(password))
	sha.Reset()
	_, err = sha.Write(hash1)
	if err != nil {
		logutil.Errorf("SHA1(SHA1(password)) failed.")
		return false
	}
	hash2 := sha.Sum(nil)

	//hash3 = SHA1(salt + SHA1(SHA1(password)))
	sha.Reset()
	_, err = sha.Write(salt)
	if err != nil {
		logutil.Errorf("write salt failed.")
		return false
	}
	_, err = sha.Write(hash2)
	if err != nil {
		logutil.Errorf("write SHA1(SHA1(password)) failed.")
		return false
	}
	hash3 := sha.Sum(nil)

	//SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	for i := range hash1 {
		hash1[i] ^= hash3[i]
	}

	logutil.Infof("server calculated %v\n", hash1)
	logutil.Infof("client calculated %v\n", auth)

	return bytes.Equal(hash1, auth)
}

//the server authenticate that the client can connect and use the database
func (tR *TestRoutine) authenticateUser(authResponse []byte) error {
	//TO Check password
	if tR.checkPassword([]byte("111"), tR.salt, authResponse) {
		logutil.Infof("check password succeeded\n")
	} else {
		return fmt.Errorf("check password failed\n")
	}
	return nil
}

func (tR *TestRoutine) setSequenceID(value uint8) {
	tR.sequenceId = value
}

//the server makes a handshake v10 packet
//return handshake packet
func (tR *TestRoutine) makeHandshakeV10Payload() []byte {
	var data = make([]byte, 256)
	var pos = 0
	//int<1> protocol version
	pos = tR.ioPackage.WriteUint8(data, pos, clientProtocolVersion)

	//string[NUL] server version
	pos = tR.writeStringNUL(data, pos, serverVersion)

	//int<4> connection id
	pos = tR.ioPackage.WriteUint32(data, pos, tR.connectionID)

	//string[8] auth-plugin-data-part-1
	pos = tR.writeCountOfBytes(data, pos, tR.salt[0:8])

	//int<1> filler 0
	pos = tR.ioPackage.WriteUint8(data, pos, 0)

	//int<2>              capabilities flags (lower 2 bytes)
	pos = tR.ioPackage.WriteUint16(data, pos, uint16(DefaultCapability&0xFFFF))

	//int<1>              character set
	pos = tR.ioPackage.WriteUint8(data, pos, utf8mb4BinCollationID)

	//int<2>              status flags
	pos = tR.ioPackage.WriteUint16(data, pos, DefaultClientConnStatus)

	//int<2>              capabilities flags (upper 2 bytes)
	pos = tR.ioPackage.WriteUint16(data, pos, uint16((DefaultCapability>>16)&0xFFFF))

	//int<1>              length of auth-plugin-data
	//set 21 always
	pos = tR.ioPackage.WriteUint8(data, pos, 21)

	//string[10]     reserved (all [00])
	pos = tR.writeZeros(data, pos, 10)

	//string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
	pos = tR.writeCountOfBytes(data, pos, tR.salt[8:])
	pos = tR.ioPackage.WriteUint8(data, pos, 0)

	//string[NUL]    auth-plugin name
	pos = tR.writeStringNUL(data, pos, AuthNativePassword)

	return data[:pos]
}

//the server analyses handshake response41 info from the client
//return true - analysed successfully / false - failed ; response41 ; error
func (tR *TestRoutine) analyseHandshakeResponse41(data []byte) (bool, response41, error) {
	var pos = 0
	var ok bool
	var info response41

	//int<4>             capabilities flags of the client, CLIENT_PROTOCOL_41 always set
	info.capabilities, pos, ok = tR.ioPackage.ReadUint32(data, pos)
	if !ok {
		return false, info, fmt.Errorf("get capabilities failed")
	}

	if (info.capabilities & CLIENT_PROTOCOL_41) == 0 {
		return false, info, fmt.Errorf("capabilities does not have protocol 41")
	}

	//int<4>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxPacketSize, pos, ok = tR.ioPackage.ReadUint32(data, pos)
	if !ok {
		return false, info, fmt.Errorf("get max packet size failed")
	}

	//int<1>             character set
	//connection's default character set
	info.collationID, pos, ok = tR.ioPackage.ReadUint8(data, pos)
	if !ok {
		return false, info, fmt.Errorf("get character set failed")
	}

	//string[23]         reserved (all [0])
	//just skip it
	pos += 23

	//string[NUL]        username
	info.username, pos, ok = tR.readStringNUL(data, pos)
	if !ok {
		return false, info, fmt.Errorf("get username failed")
	}

	/*
		if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
			lenenc-int         length of auth-response
			string[n]          auth-response
		} else if capabilities & CLIENT_SECURE_CONNECTION {
			int<1>             length of auth-response
			string[n]           auth-response
		} else {
			string[NUL]        auth-response
		}
	*/
	if (info.capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
		var l uint64
		l, pos, ok = tR.readIntLenEnc(data, pos)
		if !ok {
			return false, info, fmt.Errorf("get length of auth-response failed")
		}
		info.authResponse, pos, ok = tR.readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, fmt.Errorf("get auth-response failed")
		}
	} else if (info.capabilities & CLIENT_SECURE_CONNECTION) != 0 {
		var l uint8
		l, pos, ok = tR.ioPackage.ReadUint8(data, pos)
		if !ok {
			return false, info, fmt.Errorf("get length of auth-response failed")
		}
		info.authResponse, pos, ok = tR.readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, fmt.Errorf("get auth-response failed")
		}
	} else {
		var auth string
		auth, pos, ok = tR.readStringNUL(data, pos)
		if !ok {
			return false, info, fmt.Errorf("get auth-response failed")
		}
		info.authResponse = []byte(auth)
	}

	if (info.capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
		info.database, pos, ok = tR.readStringNUL(data, pos)
		if !ok {
			return false, info, fmt.Errorf("get database failed")
		}
	}

	if (info.capabilities & CLIENT_PLUGIN_AUTH) != 0 {
		info.clientPluginName, pos, ok = tR.readStringNUL(data, pos)
		if !ok {
			return false, info, fmt.Errorf("get auth plugin name failed")
		}

		//to switch authenticate method
		if info.clientPluginName != AuthNativePassword {
			var err error
			if info.authResponse, err = tR.negotiateAuthenticationMethod(); err != nil {
				return false, info, fmt.Errorf("negotiate authentication method failed. error:%v", err)
			}
			info.clientPluginName = AuthNativePassword
		}
	}

	//drop client connection attributes
	return true, info, nil
}

//the server does something after receiving a handshake response41 from the client
//like check user and password
//and other things
func (tR *TestRoutine) handleClientResponse41(resp41 response41) error {
	//to do something else
	//fmt.Printf("capabilities 0x%x\n", resp41.capabilities)
	//fmt.Printf("maxPacketSize %d\n", resp41.maxPacketSize)
	//fmt.Printf("collationID %d\n", resp41.collationID)
	//fmt.Printf("username %s\n", resp41.username)
	//fmt.Printf("authResponse: \n")
	//update the capabilities with client's capabilities
	tR.capability = DefaultCapability & resp41.capabilities

	//character set
	if nameAndCharset, ok := collationID2CharsetAndName[int(resp41.collationID)]; !ok {
		return fmt.Errorf("get collationName and charset failed")
	} else {
		tR.collationID = int(resp41.collationID)
		tR.collationName = nameAndCharset.collationName
		tR.charset = nameAndCharset.charset
	}

	tR.maxClientPacketSize = resp41.maxPacketSize
	tR.username = resp41.username
	tR.database = resp41.database

	//fmt.Printf("collationID %d collatonName %s charset %s \n", tR.collationID, tR.collationName, tR.charset)
	//fmt.Printf("database %s \n", resp41.database)
	//fmt.Printf("clientPluginName %s \n", resp41.clientPluginName)
	return nil
}

//the server makes a AuthSwitchRequest that asks the client to authenticate the data with new method
func (tR *TestRoutine) makeAuthSwitchRequestPayload(authMethodName string) []byte {
	var data = make([]byte, 1+len(authMethodName)+1+len(tR.salt)+1)
	pos := tR.ioPackage.WriteUint8(data, 0, defines.EOFHeader)
	pos = tR.writeStringNUL(data, pos, authMethodName)
	pos = tR.writeCountOfBytes(data, pos, tR.salt)
	pos = tR.ioPackage.WriteUint8(data, pos, 0)
	return data
}

//the server can send AuthSwitchRequest to ask client to use designated authentication method,
//if both server and client support CLIENT_PLUGIN_AUTH capability.
//return data authenticated with new method
func (tR *TestRoutine) negotiateAuthenticationMethod() ([]byte, error) {
	var err error
	aswPkt := tR.makeAuthSwitchRequestPayload(AuthNativePassword)
	err = tR.writePackets(aswPkt)
	if err != nil {
		return nil, err
	}

	read, err := tR.io.Read()

	data := read.(*Packet).Payload
	tR.sequenceId++
	return data, nil
}

//make a OK packet
func (tR *TestRoutine) makeOKPayload(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	var data = make([]byte, 128)
	var pos = 0
	pos = tR.ioPackage.WriteUint8(data, pos, defines.OKHeader)
	pos = tR.writeIntLenEnc(data, pos, affectedRows)
	pos = tR.writeIntLenEnc(data, pos, lastInsertId)
	if (tR.capability & CLIENT_PROTOCOL_41) != 0 {
		pos = tR.ioPackage.WriteUint16(data, pos, statusFlags)
		pos = tR.ioPackage.WriteUint16(data, pos, warnings)
	} else if (tR.capability & CLIENT_TRANSACTIONS) != 0 {
		pos = tR.ioPackage.WriteUint16(data, pos, statusFlags)
	}

	if tR.capability&CLIENT_SESSION_TRACK != 0 {
		//TODO:implement it
	} else {
		alen := Min(len(data)-pos, len(message))
		blen := len(message) - alen
		if alen > 0 {
			pos = tR.writeStringFix(data, pos, message, alen)
		}
		if blen > 0 {
			return tR.appendStringFix(data, message, blen)
		}
		return data[:pos]
	}
	return data
}

//send OK packet to the client
func (tR *TestRoutine) sendOKPacket(affectedRows, lastInsertId uint64, status, warnings uint16) error {
	okPkt := tR.makeOKPayload(affectedRows, lastInsertId, status, warnings, "")
	return tR.writePackets(okPkt)
}

//make Err packet
func (tR *TestRoutine) makeErrPayload(errorCode uint16, sqlState, errorMessage string) []byte {
	var data = make([]byte, 9+len(errorMessage))
	pos := tR.ioPackage.WriteUint8(data, 0, defines.ErrHeader)
	pos = tR.ioPackage.WriteUint16(data, pos, errorCode)
	if tR.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = tR.ioPackage.WriteUint8(data, pos, '#')
		if len(sqlState) < 5 {
			stuff := "      "
			sqlState += stuff[:5-len(sqlState)]
		}
		pos = tR.writeStringFix(data, pos, sqlState, 5)
	}
	pos = tR.writeStringFix(data, pos, errorMessage, len(errorMessage))
	return data
}

/*
the server sends the Error packet

information from https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
mysql version 8.0.23
usually it is in the directory /usr/local/include/mysql/mysqld_error.h

Error information includes several elements: an error code, SQLSTATE value, and message string.
	Error code: This value is numeric. It is MySQL-specific and is not portable to other database systems.
	SQLSTATE value: This value is a five-character string (for example, '42S02'). SQLSTATE values are taken from ANSI SQL and ODBC and are more standardized than the numeric error codes.
	Message string: This string provides a textual description of the error.
*/
func (tR *TestRoutine) sendErrPacket(errorCode uint16, sqlState, errorMessage string) error {
	errPkt := tR.makeErrPayload(errorCode, sqlState, errorMessage)
	return tR.writePackets(errPkt)
}

func (tR *TestRoutine) makeEOFPayload(warnings, status uint16) []byte {
	data := make([]byte, 10)
	pos := tR.ioPackage.WriteUint8(data, 0, defines.EOFHeader)
	if tR.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = tR.ioPackage.WriteUint16(data, pos, warnings)
		pos = tR.ioPackage.WriteUint16(data, pos, status)
	}
	return data[:pos]
}

func (tR *TestRoutine) sendEOFPacket(warnings, status uint16) error {
	data := tR.makeEOFPayload(warnings, status)
	return tR.writePackets(data)
}

func (tR *TestRoutine) SendEOFPacketIf(warnings, status uint16) error {
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is not set, EOF_Packet
	if tR.capability&CLIENT_DEPRECATE_EOF == 0 {
		return tR.sendEOFPacket(warnings, status)
	}
	return nil
}

//the OK or EOF packet
//thread safe
func (tR *TestRoutine) sendEOFOrOkPacket(warnings, status uint16) error {
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is set, OK_Packet; else EOF_Packet.
	if tR.capability&CLIENT_DEPRECATE_EOF != 0 {
		return tR.sendOKPacket(0, 0, status, 0)
	} else {
		return tR.sendEOFPacket(warnings, status)
	}
}

//make the column information with the format of column definition41
func (tR *TestRoutine) makeColumnDefinition41Payload(column *MysqlColumn, cmd int) []byte {
	space := 8*9 + //lenenc bytes of 8 fields
		21 + //fixed-length fields
		3 + // catalog "def"
		len(column.Schema()) +
		len(column.Table()) +
		len(column.OrgTable()) +
		len(column.Name()) +
		len(column.OrgName()) +
		len(column.DefaultValue()) +
		100 // for safe

	data := make([]byte, space)

	//lenenc_str     catalog(always "def")
	pos := tR.writeStringLenEnc(data, 0, "def")

	//lenenc_str     schema
	pos = tR.writeStringLenEnc(data, pos, column.Schema())

	//lenenc_str     table
	pos = tR.writeStringLenEnc(data, pos, column.Table())

	//lenenc_str     org_table
	pos = tR.writeStringLenEnc(data, pos, column.OrgTable())

	//lenenc_str     name
	pos = tR.writeStringLenEnc(data, pos, column.Name())

	//lenenc_str     org_name
	pos = tR.writeStringLenEnc(data, pos, column.OrgName())

	//lenenc_int     length of fixed-length fields [0c]
	pos = tR.ioPackage.WriteUint8(data, pos, 0x0c)

	//int<2>              character set
	pos = tR.ioPackage.WriteUint16(data, pos, column.Charset())

	//int<4>              column length
	pos = tR.ioPackage.WriteUint32(data, pos, column.Length())

	//int<1>              type
	pos = tR.ioPackage.WriteUint8(data, pos, column.ColumnType())

	//int<2>              flags
	pos = tR.ioPackage.WriteUint16(data, pos, column.Flag())

	//int<1>              decimals
	pos = tR.ioPackage.WriteUint8(data, pos, column.Decimal())

	//int<2>              filler [00] [00]
	pos = tR.ioPackage.WriteUint16(data, pos, 0)

	if uint8(cmd) == COM_FIELD_LIST {
		pos = tR.writeIntLenEnc(data, pos, uint64(len(column.DefaultValue())))
		pos = tR.writeCountOfBytes(data, pos, column.DefaultValue())
	}

	return data[:pos]
}

// SendColumnDefinitionPacket the server send the column definition to the client
func (tR *TestRoutine) SendColumnDefinitionPacket(column Column, cmd int) error {
	mysqlColumn, ok := column.(*MysqlColumn)
	if !ok {
		return fmt.Errorf("sendColumn need MysqlColumn")
	}

	var data []byte
	if tR.capability&CLIENT_PROTOCOL_41 != 0 {
		data = tR.makeColumnDefinition41Payload(mysqlColumn, cmd)
	}

	return tR.writePackets(data)
}

// SendColumnCountPacket makes the column count packet
func (tR *TestRoutine) SendColumnCountPacket(count uint64) error {
	data := make([]byte, 20)
	pos := tR.writeIntLenEnc(data, 0, count)

	return tR.writePackets(data[:pos])
}

func (tR *TestRoutine) sendColumns(mrs *MysqlResultSet, cmd int, warnings, status uint16) error {
	//column_count * Protocol::ColumnDefinition packets
	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		var col Column
		col, err := mrs.GetColumn(i)
		if err != nil {
			return err
		}

		err = tR.SendColumnDefinitionPacket(col, cmd)
		if err != nil {
			return err
		}
	}

	//If the CLIENT_DEPRECATE_EOF client capabilities flag is not set, EOF_Packet
	if tR.capability&CLIENT_DEPRECATE_EOF == 0 {
		err := tR.sendEOFPacket(warnings, status)
		if err != nil {
			return err
		}
	}
	return nil
}

//the server convert every row of the result set into the format that mysql protocol needs
func (tR *TestRoutine) makeResultSetTextRow(mrs *MysqlResultSet, r uint64) ([]byte, error) {
	var data []byte
	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		column, err := mrs.GetColumn(i)
		if err != nil {
			return nil, err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return nil, fmt.Errorf("sendColumn need MysqlColumn")
		}

		if isNil, err1 := mrs.ColumnIsNull(r, i); err1 != nil {
			return nil, err1
		} else if isNil {
			//NULL is sent as 0xfb
			data = tR.ioPackage.AppendUint8(data, 0xFB)
			continue
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_DECIMAL:
			return nil, fmt.Errorf("unsupported Decimal")
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			if value, err2 := mrs.GetInt64(r, i); err2 != nil {
				return nil, err2
			} else {
				if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
					if value == 0 {
						data = tR.appendStringLenEnc(data, "0000")
					} else {
						data = tR.appendStringLenEncOfInt64(data, value)
					}
				} else {
					data = tR.appendStringLenEncOfInt64(data, value)
				}
			}
		case defines.MYSQL_TYPE_FLOAT:
			if value, err2 := mrs.GetFloat64(r, i); err2 != nil {
				return nil, err2
			} else {
				data = tR.appendStringLenEncOfFloat64(data, value, 32)
			}
		case defines.MYSQL_TYPE_DOUBLE:
			if value, err2 := mrs.GetFloat64(r, i); err2 != nil {
				return nil, err2
			} else {
				data = tR.appendStringLenEncOfFloat64(data, value, 64)
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err2 := mrs.GetUint64(r, i); err2 != nil {
					return nil, err2
				} else {
					data = tR.appendStringLenEncOfUint64(data, value)
				}
			} else {
				if value, err2 := mrs.GetInt64(r, i); err2 != nil {
					return nil, err2
				} else {
					data = tR.appendStringLenEncOfInt64(data, value)
				}
			}
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING:
			if value, err2 := mrs.GetString(r, i); err2 != nil {
				return nil, err2
			} else {
				data = tR.appendStringLenEnc(data, value)
			}
		case defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_TIME:
			return nil, fmt.Errorf("unsupported DATE/DATETIME/TIMESTAMP/MYSQL_TYPE_TIME")
		default:
			return nil, fmt.Errorf("unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	return data, nil
}

//the server send group row of the result set as an independent packet
//thread safe
func (tR *TestRoutine) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	if cnt == 0 {
		return nil
	}

	var err error = nil
	for i := uint64(0); i < cnt; i++ {
		if err = tR.sendResultSetTextRow(mrs, i); err != nil {
			return err
		}
	}
	return err
}

//the server send every row of the result set as an independent packet
//thread safe
func (tR *TestRoutine) SendResultSetTextRow(mrs *MysqlResultSet, r uint64) error {
	return tR.sendResultSetTextRow(mrs, r)
}

//the server send every row of the result set as an independent packet
func (tR *TestRoutine) sendResultSetTextRow(mrs *MysqlResultSet, r uint64) error {
	var data []byte
	var err error
	if data, err = tR.makeResultSetTextRow(mrs, r); err != nil {
		//ERR_Packet in case of error
		err1 := tR.sendErrPacket(ER_UNKNOWN_ERROR, DefaultMySQLState, err.Error())
		if err1 != nil {
			return err1
		}
		return err
	}

	err = tR.writePackets(data)
	if err != nil {
		return fmt.Errorf("send result set text row failed. error: %v", err)
	}
	return nil
}

//the server send the result set of execution the client
//the routine follows the article: https://dev.mysql.com/doc/internals/en/com-query-response.html
func (tR *TestRoutine) sendResultSet(set ResultSet, cmd int, warnings, status uint16) error {
	mysqlRS, ok := set.(*MysqlResultSet)
	if !ok {
		return fmt.Errorf("sendResultSet need MysqlResultSet")
	}

	//A packet containing a Protocol::LengthEncodedInteger column_count
	err := tR.SendColumnCountPacket(mysqlRS.GetColumnCount())
	if err != nil {
		return err
	}

	if err = tR.sendColumns(mysqlRS, cmd, warnings, status); err != nil {
		return err
	}

	//One or more ProtocolText::ResultsetRow packets, each containing column_count values
	for i := uint64(0); i < mysqlRS.GetRowCount(); i++ {
		if err = tR.sendResultSetTextRow(mysqlRS, i); err != nil {
			return err
		}
	}

	//If the CLIENT_DEPRECATE_EOF client capabilities flag is set, OK_Packet; else EOF_Packet.
	if tR.capability&CLIENT_DEPRECATE_EOF != 0 {
		err := tR.sendOKPacket(0, 0, status, 0)
		if err != nil {
			return err
		}
	} else {
		err := tR.sendEOFPacket(warnings, status)
		if err != nil {
			return err
		}
	}

	return nil
}

//the server sends the payload to the client
func (tR *TestRoutine) writePackets(payload []byte) error {
	var i = 0
	var length = len(payload)
	var curLen = 0
	for ; i < length; i += curLen {
		var packet []byte
		curLen = Min(int(MaxPayloadSize), length-i)

		//make mysql client protocol header
		//4 bytes
		//int<3>    the length of payload
		var header [4]byte
		tR.ioPackage.WriteUint32(header[:], 0, uint32(curLen))

		//int<1> sequence id
		tR.ioPackage.WriteUint8(header[:], 3, tR.sequenceId)

		//send packet
		packet = append(packet, header[:]...)
		packet = append(packet, payload[i : i + curLen]...)
		err := tR.io.WriteAndFlush(packet)
		if err != nil {
			return err
		}

		tR.sequenceId++

		if i + curLen == length && curLen == int(MaxPayloadSize) {
			//if the size of the last packet is exactly MaxPayloadSize, a zero-size payload should be sent
			header[0] = 0
			header[1] = 0
			header[2] = 0
			header[3] = tR.sequenceId

			//send header / zero-sized packet
			err := tR.io.WriteAndFlush(header[:])
			if err != nil {
				return err
			}

			tR.sequenceId++
		}
	}
	return nil
}

func NewTestRoutine(rs goetty.IOSession)*TestRoutine {
	salt := make([]byte, 20)
	rand.Read(salt)
	ri := &TestRoutine{
		io:          rs,
		established: false,
		ioPackage: NewIOPackage(true),
		salt: salt,
	}

	return ri
}

type TestRoutineManager struct {
	rwlock  sync.RWMutex
	clients map[goetty.IOSession]*TestRoutine
}

func (tRM *TestRoutineManager) Created(rs goetty.IOSession) {
	fmt.Println("Created")
	routine := NewTestRoutine(rs)

	hsV10pkt := routine.makeHandshakeV10Payload()
	err := routine.writePackets(hsV10pkt)
	if err != nil {
		panic(err)
	}

	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	tRM.clients[rs] = routine
}

func (tRM *TestRoutineManager) Closed(rs goetty.IOSession) {
	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	delete(tRM.clients, rs)
}

func NewTestRoutineManager() *TestRoutineManager {
	rm := &TestRoutineManager{
		clients: make(map[goetty.IOSession]*TestRoutine),
	}
	return rm
}

func TestReadIntLenEnc(t *testing.T) {
	var intEnc MysqlProtocol
	var data = make([]byte, 24)
	var cases = [][]uint64{
		{0, 123, 250},
		{251, 10000, 1<<16 - 1},
		{1 << 16, 1<<16 + 10000, 1<<24 - 1},
		{1 << 24, 1<<24 + 10000, 1<<64 - 1},
	}
	var caseLens = []int{1, 3, 4, 9}
	for j := 0; j < len(cases); j++ {
		for i := 0; i < len(cases[j]); i++ {
			value := cases[j][i]
			p1 := intEnc.writeIntLenEnc(data, 0, value)
			val, p2, ok := intEnc.readIntLenEnc(data, 0)
			if !ok || p1 != caseLens[j] || p1 != p2 || val != value {
				t.Errorf("IntLenEnc %d failed.", value)
				break
			}
			val, p2, ok = intEnc.readIntLenEnc(data[0:caseLens[j]-1], 0)
			if ok {
				t.Errorf("read IntLenEnc failed.")
				break
			}
		}
	}
}

func TestReadCountOfBytes(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	for i := 0; i < length; i++ {
		data[i] = byte(length - i)
	}

	r, pos, ok := client.readCountOfBytes(data, 0, length)
	if !ok || pos != length {
		t.Error("read bytes failed.")
		return
	}

	for i := 0; i < length; i++ {
		if r[i] != data[i] {
			t.Error("read != write")
			break
		}
	}

	r, pos, ok = client.readCountOfBytes(data, 0, 100)
	if ok {
		t.Error("read bytes failed.")
		return
	}

	r, pos, ok = client.readCountOfBytes(data, 0, 0)
	if !ok || pos != 0 {
		t.Error("read bytes failed.")
		return
	}
}

func TestReadStringFix(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringFix(data, 0, s, length)
	if pos != length {
		t.Error("write string fix failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringFix(data, 0, length)
	if !ok || pos != length || x != s[0:length] {
		t.Error("read string fix failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringFix(data, 0, sLen[i])
		if ok && pos == sLen[i] && x == s[0:sLen[i]] {
			t.Error("read string fix failed.")
			return
		}
	}

	//empty string
	pos = client.writeStringFix(data, 0, s, 0)
	if pos != 0 {
		t.Error("write string fix failed.")
		return
	}

	x, pos, ok = client.readStringFix(data, 0, 0)
	if !ok || pos != 0 || x != "" {
		t.Error("read string fix failed.")
		return
	}
}

func TestReadStringNUL(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringNUL(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string NUL failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringNUL(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string NUL failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringNUL(data, 0)
		if ok && pos == sLen[i]+1 && x == s[0:sLen[i]] {
			t.Error("read string NUL failed.")
			return
		}
	}
}

func TestReadStringLenEnc(t *testing.T) {
	var client MysqlProtocol
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringLenEnc(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string lenenc failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string lenenc failed.")
		return
	}

	//empty string
	pos = client.writeStringLenEnc(data, 0, s[0:0])
	if pos != 1 {
		t.Error("write string lenenc failed.")
		return
	}

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != 1 || x != s[0:0] {
		t.Error("read string lenenc failed.")
		return
	}
}
/*
func handshakeHandler(in goetty.IOSession){
	var err error
	io := NewIOPackage(true)

	fmt.Println("Server handling")
	mysql := NewMysqlClientProtocol(io,0)
	if err = mysql.Handshake(); err!=nil{
		msg := fmt.Sprintf("handshake failed. error:%v",err)
		mysql.sendErrPacket(ER_UNKNOWN_ERROR,DefaultMySQLState,msg)
		return
	}

	var req *Request
	var resp *Response
	for{
		//The sequence-id is incremented with each packet and may wrap around.
		//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
		mysql.setSequenceID(0)

		if req,err =mysql.ReadRequest(); err!=nil{
			fmt.Printf("read request failed. error:%v",err)
			break
		}
		switch uint8(req.GetCmd()){
		case COM_QUIT:
			resp = &Response{
				category: OkResponse,
				status:   0,
				data:     nil,
			}
			if err = mysql.SendResponse(resp); err != nil{
				fmt.Printf("send response failed. error:%v",err)
				break
			}
		case COM_QUERY:
			var query =string(req.GetData().([]byte))
			fmt.Printf("query: %s \n",query)
			resp = &Response{
				category: OkResponse,
				status:   0,
				data:     nil,
			}
			if err = mysql.SendResponse(resp); err != nil{
				fmt.Printf("send response failed. error:%v",err)
				break
			}
		default:
			fmt.Printf("unsupported command. 0x%x \n",req.cmd)
		}
		if uint8(req.cmd) == COM_QUIT{
			break
		}
	}

}
*/
func TestMysqlClientProtocol_Handshake(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connection method: mysql -h 127.0.0.1 -P 6001 -udump -p
	//echoServer(handshakeHandler)

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	if err := config.LoadvarsConfigFromFile("../../system_vars_config.toml",
		&config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)

	ppu := NewPDCallbackParameterUnit(int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()), int(config.GlobalSystemVariables.GetPeriodOfPersistence()), int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()), int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()), config.GlobalSystemVariables.GetEnableEpochLogging())
	pci := NewPDCallbackImpl(ppu)
	pci.Id = 0
	rm := NewRoutineManager(pu, pci)

	encoder, decoder := NewSqlCodec()
	echoServer(rm.Handler, rm, encoder, decoder)
}

func makeMysqlTinyIntResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Tiny"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_TINY)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint8{0,1,254,255}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int8{-128,-127,127}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlTinyResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlTinyIntResultSet(unsigned))
}

func makeMysqlShortResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Short"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint16{0,1,254,255,65535}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int16{-32768,0,32767}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlShortResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlShortResultSet(unsigned))
}

func makeMysqlLongResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Long"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint32{0,4294967295}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int32{-2147483648,0,2147483647}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlLongResultSet(unsigned))
}

func makeMysqlLongLongResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "LongLong"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint64{0,4294967295,18446744073709551615}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int64{-9223372036854775808,0,9223372036854775807}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongLongResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlLongLongResultSet(unsigned))
}

func makeMysqlInt24ResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Int24"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_INT24)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		//[0,16777215]
		var cases=[]uint32{0,16777215,4294967295}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		//[-8388608,8388607]
		var cases=[]int32{-2147483648,-8388608,0,8388607,2147483647}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlInt24Result(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlInt24ResultSet(unsigned))
}

func makeMysqlYearResultSet(unsigned bool)*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Year"
	if unsigned{
		name = name + "Uint"
	}else{
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_YEAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned{
		var cases=[]uint16{0,1,254,255,65535}
		for _, v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}else{
		var cases=[]int16{-32768,0,32767}
		for _,v := range cases{
			var data = make([]interface{},1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlYearResult(unsigned bool) *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlYearResultSet(unsigned))
}

func makeMysqlVarcharResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varchar"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlVarcharResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlVarcharResultSet())
}

func makeMysqlVarStringResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlVarStringResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlVarStringResultSet())
}

func makeMysqlStringResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "String"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]string{"abc","abcde","","x-","xx"}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlStringResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlStringResultSet())
}

func makeMysqlFloatResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Float"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]float32{math.MaxFloat32,math.SmallestNonzeroFloat32,-math.MaxFloat32,-math.SmallestNonzeroFloat32}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlFloatResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlFloatResultSet())
}

func makeMysqlDoubleResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Double"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases=[]float64{math.MaxFloat64,math.SmallestNonzeroFloat64,-math.MaxFloat64,-math.SmallestNonzeroFloat64}
	for _,v := range cases{
		var data = make([]interface{},1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlDoubleResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMysqlDoubleResultSet())
}

func make8ColumnsResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []uint8{
		defines.MYSQL_TYPE_TINY,
		defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_VARCHAR,
		defines.MYSQL_TYPE_FLOAT}

	var names=[]string{
		"Tiny",
		"Short",
		"Long",
		"Longlong",
		"Varchar",
		"Float",
	}

	var cases=[][]interface{}{
		{-128,-32768,-2147483648,-9223372036854775808,"abc",math.MaxFloat32},
		{-127,0,    0,0,"abcde",math.SmallestNonzeroFloat32},
		{127,32767,2147483647,9223372036854775807,"",-math.MaxFloat32},
		{126,32766,2147483646,9223372036854775806,"x-",-math.SmallestNonzeroFloat32},
	}

	for i,ct := range columnTypes{
		name := names[i]
		mysqlCol := new(MysqlColumn)
		mysqlCol.SetName(name)
		mysqlCol.SetOrgName(name + "OrgName")
		mysqlCol.SetColumnType(ct)
		mysqlCol.SetSchema(name + "Schema")
		mysqlCol.SetTable(name + "Table")
		mysqlCol.SetOrgTable(name + "Table")
		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

		rs.AddColumn(mysqlCol)
	}

	for _,v := range cases{
		rs.AddRow(v)
	}

	return rs
}

func makeMysql8ColumnsResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,make8ColumnsResultSet())
}

func makeMoreThan16MBResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []uint8{
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_VARCHAR,
	}

	var names=[]string{
		"Longlong",
		"Double",
		"Varchar",
	}

	var rowCase =[]interface{}{9223372036854775807,math.MaxFloat64,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

	for i,ct := range columnTypes{
		name := names[i]
		mysqlCol := new(MysqlColumn)
		mysqlCol.SetName(name)
		mysqlCol.SetOrgName(name + "OrgName")
		mysqlCol.SetColumnType(ct)
		mysqlCol.SetSchema(name + "Schema")
		mysqlCol.SetTable(name + "Table")
		mysqlCol.SetOrgTable(name + "Table")
		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

		rs.AddColumn(mysqlCol)
	}

	//the size of the total result set will be more than 16MB
	for i := 0 ; i < 40000; i++{
		rs.AddRow(rowCase)
	}

	return rs
}

//the size of resultset will be morethan 16MB
func makeMoreThan16MBResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,makeMoreThan16MBResultSet())
}

func make16MBRowResultSet()*MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	/*
		How to test the max size of the data in one packet that the client can received ?
		Environment: Mysql Version 8.0.23
		1. shell: mysql --help | grep allowed-packet
			something like:
			"
			  --max-allowed-packet=#
			max-allowed-packet                16777216
			"
			so, we get:
				max-allowed-packet means : The maximum packet length to send to or receive from server.
				default value : 16777216 (16MB)
		2. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-4);" > 16MB-mysql.txt
			we get: ERROR 2020 (HY000) at line 1: Got packet bigger than 'max_allowed_packet' bytes
		3. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-5);" > 16MB-mysql.txt
			execution succeeded
		4. so, the max size of the data in one packet is (max-allowed-packet - 5).
		5. To change max-allowed-packet.
			shell execution: mysql max-allowed-packet=xxxxx ....
	*/


	//test in shell : mysql -h 127.0.0.1 -P 6001 -udump -p111 -e "16mbrow" > 16mbrow.txt
	//max data size : 16 * 1024 * 1024 - 5
	var stuff = make([]byte, 16 * 1024 * 1024 - 5)
	for i := range stuff{
		stuff[i] = 'a'
	}

	var rowCase = []interface{} {string(stuff)}
	for i := 0 ; i < 1; i++{
		rs.AddRow(rowCase)
	}

	return rs
}

//the size of resultset row will be more than 16MB
func make16MBRowResult() *MysqlExecutionResult {
	return NewMysqlExecutionResult(0,0,0,0,make16MBRowResultSet())
}

func (tR *TestRoutine) handleHandshake(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("received a broken response packet")
	}

	var authResponse []byte
	if capabilities, _, ok := tR.ioPackage.ReadUint16(payload, 0); !ok {
		return fmt.Errorf("read capabilities from response packet failed")
	} else if uint32(capabilities)&CLIENT_PROTOCOL_41 != 0 {
		var resp41 response41
		var ok bool
		var err error
		if ok, resp41, err = tR.analyseHandshakeResponse41(payload); !ok {
			return err
		}

		authResponse = resp41.authResponse
		tR.capability = DefaultCapability & resp41.capabilities

		if nameAndCharset, ok := collationID2CharsetAndName[int(resp41.collationID)]; !ok {
			return fmt.Errorf("get collationName and charset failed")
		} else {
			tR.collationID = int(resp41.collationID)
			tR.collationName = nameAndCharset.collationName
			tR.charset = nameAndCharset.charset
		}

		tR.maxClientPacketSize = resp41.maxPacketSize
		tR.username = resp41.username
	}

	if err := tR.authenticateUser(authResponse); err != nil {
		fail := errorMsgRefer[ER_ACCESS_DENIED_ERROR]
		_ = tR.sendErrPacket(fail.errorCode, fail.sqlStates[0], "Access denied for user")
		return err
	}

	err := tR.sendOKPacket(0, 0, 0, 0)
	if err != nil {
		return err
	}
	fmt.Println("SWITCH ESTABLISHED to true")
	tR.established = true
	return nil
}

func (tR *TestRoutine) GetRequest(payload []byte) *Request {
	req := &Request{
		cmd:  int(payload[0]),
		data: payload[1:],
	}

	return req
}

func (tR *TestRoutine) SendResponse(resp *Response) error {
	switch resp.category {
	case OkResponse:
		return tR.sendOKPacket(0, 0, uint16(resp.status), 0)
	case EoFResponse:
		return tR.sendEOFPacket(0, uint16(resp.status))
	case ErrorResponse:
		err := resp.data.(error)
		if err == nil {
			return tR.sendOKPacket(0, 0, uint16(resp.status), 0)
		}
		switch myerr := err.(type) {
		case *MysqlError:
			return tR.sendErrPacket(myerr.ErrorCode, myerr.SqlState, myerr.Error())
		}
		return tR.sendErrPacket(ER_UNKNOWN_ERROR, DefaultMySQLState, fmt.Sprintf("unkown error:%v", err))
	case ResultResponse:
		mer := resp.data.(*MysqlExecutionResult)
		if mer == nil {
			return tR.sendOKPacket(0, 0, uint16(resp.status), 0)
		}
		if mer.Mrs() == nil {
			return tR.sendOKPacket(mer.AffectedRows(), mer.InsertID(), uint16(resp.status), mer.Warnings())
		}
		return tR.sendResultSet(mer.Mrs(), resp.cmd, mer.Warnings(), uint16(resp.status))
	default:
		return fmt.Errorf("unsupported response:%d ", resp.category)
	}
}


func (tRM *TestRoutineManager)resultsetHandler(rs goetty.IOSession, msg interface{}, _ uint64) error {
	tRM.rwlock.RLock()
	routine, ok := tRM.clients[rs]
	tRM.rwlock.RUnlock()
	if !ok {
		return errors.New("routine does not exist")
	}
	packet, ok := msg.(*Packet)
	routine.sequenceId = uint8(packet.SequenceID + 1)
	if !ok {
		return errors.New("message is not Packet")
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		var err error
		msg, err = routine.io.Read()
		if err != nil {
			return errors.New("read msg error")
		}

		packet, ok = msg.(*Packet)
		if !ok {
			return errors.New("message is not Packet")
		}

		routine.sequenceId = uint8(packet.SequenceID + 1)
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !routine.established {
		fmt.Println("HANDLE HANDSHAKE")
		err := routine.handleHandshake(payload)
		if err != nil {
			return err
		}
		return nil
	}

	var req *Request
	var resp *Response
	req = routine.GetRequest(payload)
	switch uint8(req.GetCmd()) {
	case COM_QUIT:
		resp = &Response{
			category: OkResponse,
			status:   0,
			data:     nil,
		}
		if err := routine.SendResponse(resp); err != nil {
			fmt.Printf("send response failed. error:%v", err)
			break
		}
	case COM_QUERY:
		var query = string(req.GetData().([]byte))

		switch query {
		case "tiny":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				cmd:      0,
				data:     makeMysqlTinyResult(false),
			}
		case "tinyu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlTinyResult(true),
			}
		case "short":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlShortResult(false),
			}
		case "shortu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlShortResult(true),
			}
		case "long":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongResult(false),
			}
		case "longu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongResult(true),
			}
		case "longlong":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongLongResult(false),
			}
		case "longlongu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlLongLongResult(true),
			}
		case "int24":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlInt24Result(false),
			}
		case "int24u":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlInt24Result(true),
			}
		case "year":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlYearResult(false),
			}
		case "yearu":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlYearResult(true),
			}
		case "varchar":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlVarcharResult(),
			}
		case "varstring":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlVarStringResult(),
			}
		case "string":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlStringResult(),
			}
		case "float":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlFloatResult(),
			}
		case "double":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysqlDoubleResult(),
			}
		case "8columns":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMysql8ColumnsResult(),
			}
		case "16mb":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     makeMoreThan16MBResult(),
			}
		case "16mbrow":
			resp = &Response{
				category: ResultResponse,
				status:   0,
				data:     make16MBRowResult(),
			}
		default:
			resp = &Response{
				category: OkResponse,
				status:   0,
				data:     nil,
			}
		}

		if err := routine.SendResponse(resp); err != nil {
			fmt.Printf("send response failed. error:%v", err)
			break
		}

	default:
		fmt.Printf("unsupported command. 0x%x \n", req.cmd)
	}
	if uint8(req.cmd) == COM_QUIT {
		return nil
	}
	return nil
}


func TestMysqlResultSet(t *testing.T){
	//client connection method: mysql -h 127.0.0.1 -P 6001 -udump -p
	//pwd: mysql-server-mysql-8.0.23/mysql-test
	//with mysqltest: mysqltest --test-file=t/1st.test --result-file=r/1st.result --user=dump -p111 -P 6001 --host=127.0.0.1

	//test:
	//./mysql-test-run 1st --extern user=root --extern port=3306 --extern host=127.0.0.1
	//  mysql5.7 failed
	//	mysql-8.0.23 success
	//./mysql-test-run 1st --extern user=root --extern port=6001 --extern host=127.0.0.1
	//	matrixone failed: mysql-test-run: *** ERROR: Could not connect to extern server using command: '/Users/pengzhen/Documents/mysql-server-mysql-8.0.23/bld/runtime_output_directory//mysql --no-defaults --user=root --user=root --port=6001 --host=127.0.0.1 --silent --database=mysql --execute="SHOW GLOBAL VARIABLES"'
	encoder, decoder := NewSqlCodec()
	trm := NewTestRoutineManager()
	echoServer(trm.resultsetHandler, trm, encoder, decoder)
}
