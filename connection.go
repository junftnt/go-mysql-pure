package mysql

import (
	"bufio"
	"bytes"
	"io"
	"net"

	"github.com/davecgh/go-spew/spew"
)

type Connection struct {
	param ConnectionParameter
	conn  net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	debugBuf *bytes.Buffer
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
	} else {
		c.reader = bufio.NewReader(c.conn)
	}

	//
	err = c.readInitPacket()

	if err != nil {
		return err
	}

	//
	return nil
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) readInitPacket() error {
	var packetHeader *PacketHeader
	var err error

	packetHeader, err = ReadPacketHeader(c.reader)

	if err != nil {
		return err
	}

	spew.Dump(packetHeader)
	spew.Dump(c.debugBuf.Bytes())
	c.debugBuf.Reset()

	//
	return nil
}
