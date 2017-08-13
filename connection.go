package mysql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/davecgh/go-spew/spew"
)

const (
	MAX_PACKET_SIZE = (1 << 24)
)

type ClientFlags uint32

// Reference:
// https://github.com/MariaDB/mariadb-connector-c/blob/master/include/mariadb_com.h
// https://github.com/google/mysql/blob/master/include/mysql_com.h
const (
	CLIENT_LONG_PASSWORD     ClientFlags = 1       /* new more secure passwords */
	CLIENT_FOUND_ROWS                    = 2       /* Found instead of affected rows */
	CLIENT_LONG_FLAG                     = 4       /* Get all column flags */
	CLIENT_CONNECT_WITH_DB               = 8       /* One can specify db on connect */
	CLIENT_NO_SCHEMA                     = 16      /* Don't allow database.table.column */
	CLIENT_COMPRESS                      = 32      /* Can use compression protocol */
	CLIENT_ODBC                          = 64      /* Odbc client */
	CLIENT_LOCAL_FILES                   = 128     /* Can use LOAD DATA LOCAL */
	CLIENT_IGNORE_SPACE                  = 256     /* Ignore spaces before '(' */
	CLIENT_PROTOCOL_41                   = 512     /* New 4.1 protocol */
	CLIENT_INTERACTIVE                   = 1024    /* This is an interactive client */
	CLIENT_SSL                           = 2048    /* Switch to SSL after handshake */
	CLIENT_IGNORE_SIGPIPE                = 4096    /* IGNORE sigpipes */
	CLIENT_TRANSACTIONS                  = 8192    /* Client knows about transactions */
	CLIENT_RESERVED                      = 16384   /* Old flag for 4.1 protocol  */
	CLIENT_SECURE_CONNECTION             = 32768   /* New 4.1 authentication */
	CLIENT_MULTI_STATEMENTS              = 65536   /* Enable/disable multi-stmt support */
	CLIENT_MULTI_RESULTS                 = 131072  /* Enable/disable multi-results */
	CLIENT_PS_MULTI_RESULTS              = 1 << 18 /* Multi-results in PS-protocol */
	CLIENT_PLUGIN_AUTH                   = 1 << 19 /* Client supports plugin authentication */
)

const (
	MYSQL_TYPE_DECIMAL uint8 = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_NEWDECIMAL  = 246
	MYSQL_TYPE_ENUM        = 247
	MYSQL_TYPE_SET         = 248
	MYSQL_TYPE_TINY_BLOB   = 249
	MYSQL_TYPE_MEDIUM_BLOB = 250
	MYSQL_TYPE_LONG_BLOB   = 251
	MYSQL_TYPE_BLOB        = 252
	MYSQL_TYPE_VAR_STRING  = 253
	MYSQL_TYPE_STRING      = 254
	MYSQL_TYPE_GEOMETRY    = 255
)

type Connection struct {
	param ConnectionParameter
	conn  net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	mutex *sync.Mutex

	debugBuf *bytes.Buffer

	ProtocolVersion          uint8
	ServerVersion            string
	ConnectionID             uint32
	ScramblePart1            []byte
	ServerCapabilitiesPart1  uint16
	ServerDefaultCollation   uint8
	StatusFlags              uint16
	ServerCapabilitiesPart2  uint16
	LenOfScramblePart2       uint8
	ScramblePart2            []byte
	AuthenticationPluginName string
}

type ConnectionParameter struct {
	Network  string
	Host     string
	Port     string
	DBName   string
	Username string
	Password string

	IsDebugPacket bool
}

func NewConnection(param ConnectionParameter) *Connection {
	return &Connection{
		param:    param,
		mutex:    new(sync.Mutex),
		debugBuf: new(bytes.Buffer),
	}
}

func (c *Connection) Open() error {
	var err error

	c.conn, err = net.Dial(c.param.Network, c.param.Host+":"+c.param.Port)

	if err != nil {
		return err
	}

	if c.param.IsDebugPacket == true {
		c.reader = bufio.NewReader(io.TeeReader(c.conn, c.debugBuf))
		c.writer = bufio.NewWriter(io.MultiWriter(c.conn, c.debugBuf))
	} else {
		c.reader = bufio.NewReader(c.conn)
		c.writer = bufio.NewWriter(c.conn)
	}

	//
	err = c.readInitPacket()

	if err != nil {
		return err
	}

	spew.Dump(c.debugBuf.Bytes())
	c.debugBuf.Reset()

	//
	err = c.sendAuth()

	if err != nil {
		return err
	}

	spew.Dump(c.debugBuf.Bytes())
	c.debugBuf.Reset()

	//
	err = c.readResult()

	if err != nil {
		return err
	}

	spew.Dump(c.debugBuf.Bytes())
	c.debugBuf.Reset()

	//
	return nil
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

// readInitPacket reads the initial handshake packet.
// Reference:
// https://mariadb.com/kb/en/mariadb/1-connecting-connecting/#initial-handshake-packet
func (c *Connection) readInitPacket() error {
	var packetHeader *PacketHeader
	var err error

	packetHeader, err = ReadPacketHeader(c.reader)

	if err != nil {
		return err
	}

	if packetHeader.Seq != 0 {
		// The sequence number of the initial packet must be a zero.
		return errors.New("Unexpected Sequence Number")
	}

	spew.Printf("=== packetHeader\n")
	spew.Dump(packetHeader)

	// ProtocolVersion [1 byte]
	err = binary.Read(c.reader, binary.LittleEndian, &c.ProtocolVersion)

	if err != nil {
		return err
	}

	spew.Printf("=== ProtocolVersion\n")
	spew.Dump(c.ProtocolVersion)

	// ServerVersion [null terminated string]
	c.ServerVersion, err = c.reader.ReadString('\x00')

	if err != nil {
		return err
	}

	spew.Printf("=== ServerVersion\n")
	spew.Dump(c.ServerVersion)

	// ConnectionID [4 bytes]
	err = binary.Read(c.reader, binary.LittleEndian, &c.ConnectionID)

	if err != nil {
		return err
	}

	spew.Printf("=== ConnectionID\n")
	spew.Dump(c.ConnectionID)

	// ScramblePart1 [8 bytes]
	c.ScramblePart1 = make([]byte, 8)

	err = ReadPacket(c.reader, c.ScramblePart1[0:8])

	if err != nil {
		return err
	}

	spew.Printf("=== ScramblePart1\n")
	spew.Dump(c.ScramblePart1)

	// Reserved byte [1 byte]
	IgnoreBytes(c.reader, 1)

	if err != nil {
		return err
	}

	// ServerCapabilitiesPart1 (lower 2 bytes) [2 bytes]
	err = binary.Read(c.reader, binary.LittleEndian, &c.ServerCapabilitiesPart1)

	if err != nil {
		return err
	}

	// ServerDefaultCollation [1 byte]
	err = binary.Read(c.reader, binary.LittleEndian, &c.ServerDefaultCollation)

	if err != nil {
		return err
	}

	// StatusFlags [2 bytes]
	err = binary.Read(c.reader, binary.LittleEndian, &c.StatusFlags)

	if err != nil {
		return err
	}

	// ServerCapabilitiesPart2 (upper 2 bytes) [2 bytes]
	err = binary.Read(c.reader, binary.LittleEndian, &c.ServerCapabilitiesPart2)

	if err != nil {
		return err
	}

	// LenOfScramblePart2 [1 byte]
	//err = binary.Read(c.reader, binary.LittleEndian, &c.LenOfScramblePart2)

	//if err != nil {
	//	return err
	//}

	//spew.Dump(c.LenOfScramblePart2)

	// PLUGIN_AUTH [1 byte]
	// Filler [6 bytes]
	// Filler [4 bytes]
	IgnoreBytes(c.reader, 1+6+4)

	if err != nil {
		return err
	}

	// ScramblePart2 [12 bytes]
	// The web documentation is ambiguous about the length.
	// Reference:
	// https://github.com/go-sql-driver/mysql/blob/master/packets.go
	c.ScramblePart2 = make([]byte, 12)

	err = ReadPacket(c.reader, c.ScramblePart2[0:12])

	if err != nil {
		return err
	}

	spew.Printf("=== ScramblePart2\n")
	spew.Dump(c.ScramblePart2)

	// ScramblePart2 0x00
	IgnoreBytes(c.reader, 1)

	// AuthenticationPluginName [null terminated string]
	c.AuthenticationPluginName, err = c.reader.ReadString('\x00')

	if err != nil {
		return err
	}

	spew.Printf("=== AuthenticationPluginName\n")
	spew.Dump(c.AuthenticationPluginName)
	spew.Dump([]byte(c.AuthenticationPluginName))

	//
	return nil
}

// sendAuth sends the handshake response packet.
func (c *Connection) sendAuth() error {
	var err error

	//
	var clientFlags ClientFlags

	clientFlags = CLIENT_LONG_PASSWORD
	clientFlags += CLIENT_PROTOCOL_41
	clientFlags += CLIENT_SECURE_CONNECTION
	clientFlags += CLIENT_MULTI_STATEMENTS
	clientFlags += CLIENT_MULTI_RESULTS

	// client capabilities [4 bytes]
	// max packet size [4 bytes]
	// client character collation [1 byte]
	// reserved [19 bytes]
	// reserved [4 bytes]
	// username [null terminated string]
	// password length [1 byte]
	// password [fix, length is indicated by previous field]
	cipher := c.ScramblePart1
	cipher = append(cipher, c.ScramblePart2...)

	password := scramblePassword(cipher, []byte(c.param.Password))

	byteLen := 4 + 4 + 1 + 19 + 4 + (len(c.param.Username) + 1) + (1 + len(password))

	// database name [null terminated string]
	if n := len(c.param.DBName); n > 0 {
		clientFlags += CLIENT_CONNECT_WITH_DB
		byteLen += (n + 1)
	}

	// Assume native client during response [null terminated string]
	byteLen += (len("mysql_native_password") + 1)

	//
	pos := 0
	byteArr := make([]byte, byteLen+4)

	// packet length + sequence number [4 bytes]
	byteArr[0] = byte(byteLen)
	byteArr[1] = byte(byteLen >> 8)
	byteArr[2] = byte(byteLen >> 16)
	byteArr[3] = 1 // sequence number
	pos += 4

	// client capabilities [4 bytes]
	binary.LittleEndian.PutUint32(byteArr[pos:pos+4], uint32(clientFlags))
	pos += 4

	// max packet size [4 bytes]
	binary.LittleEndian.PutUint32(byteArr[pos:pos+4], uint32(MAX_PACKET_SIZE))
	pos += 4

	// client character collation [1 byte]
	byteArr[pos] = c.ServerDefaultCollation
	pos += 1

	// reserved [19 bytes]
	pos += 19

	// reserved [4 bytes]
	pos += 4

	// username [null terminated string]
	pos += copy(byteArr[pos:], c.param.Username)
	byteArr[pos] = 0x00
	pos += 1

	// ignore [1 byte]
	//pos += 1

	// password [length encoded integer]
	byteArr[pos] = byte(len(password))
	pos += 1

	// password [null terminated string]
	// TODO: password type double check
	pos += copy(byteArr[pos:], password)

	// database name [null terminated string]
	pos += copy(byteArr[pos:], c.param.DBName)
	byteArr[pos] = 0x00
	pos += 1

	// Assume native client during response [null terminated string]
	pos += copy(byteArr[pos:], "mysql_native_password")
	byteArr[pos] = 0x00
	pos += 1

	//
	_, err = c.writer.Write(byteArr[0:pos])

	if err != nil {
		return err
	}

	//
	err = c.writer.Flush()

	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) readResult() error {
	var packetHeader *PacketHeader
	var err error

	//
	packetHeader, err = ReadPacketHeader(c.reader)

	if err != nil {
		return err
	}

	spew.Dump(packetHeader)

	//
	test := make([]byte, 1)
	err = ReadPacket(c.reader, test)

	if err != nil {
		return err
	}

	return nil
}
