package server

import (
	"bufio"
	"net"
)

const defaultReaderSize = 16 * 1024

// bufferedConnReader is a net.Conn compatible structure that reads from bufio.Reader.
type bufferedConnReader struct {
	net.Conn
	rb *bufio.Reader
}

func (conn bufferedConnReader) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

func newBufferedConnReader(conn net.Conn) *bufferedConnReader {
	return &bufferedConnReader{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, defaultReaderSize),
	}
}
