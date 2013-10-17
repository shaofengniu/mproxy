package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"git.jumbo.ws/go/tcgl/applog"
)

type Server struct {
	Addr           string
	Handler        Handler
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes int
}

func (srv *Server) ListenAndServe() error {
	l, err := srv.listen()
	if err != nil {
		return err
	}
	return srv.serve(l)
}

func (srv *Server) listen() (l net.Listener, err error) {
	addr := srv.Addr
	if strings.Contains(addr, "/") {
		return net.Listen("unix", addr)
	} else {
		return net.Listen("tcp", addr)
	}
}

func (srv *Server) serve(l net.Listener) error {
	defer l.Close()
	var tempDelay time.Duration
	for {
		rw, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				applog.Errorf("Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0
		c := srv.newConn(rw)
		go c.serve()
		applog.Debugf("Accept connection from %s", c.remoteAddr)
	}
}

const noLimit int64 = (1 << 63) - 1

func (srv *Server) newConn(rwc net.Conn) (c *Conn) {
	c = new(Conn)
	c.remoteAddr = rwc.RemoteAddr().String()
	c.server = srv
	c.rwc = rwc
	br, sr := newBufioReader(c.rwc)
	bw, sw := newBufioWriterSize(c.rwc, 4<<10)
	c.buf = bufio.NewReadWriter(br, bw)
	c.bufswr = sr
	c.bufsww = sw
	return
}

type Conn struct {
	remoteAddr string
	server     *Server
	rwc        net.Conn
	buf        *bufio.ReadWriter
	bufswr     *switchReader
	bufsww     *switchWriter

	mu sync.Mutex
}

func (c *Conn) Read(p []byte) (int, error) {
	return c.buf.Reader.Read(p)
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.buf.Writer.Write(p)
}

func (c *Conn) WriteTo(w io.Writer) (int64, error) {
	return c.buf.Reader.WriteTo(c.buf.Writer)
}

func (c *Conn) Flush() error {
	return c.buf.Flush()
}

func (c *Conn) serve() {
	defer func() {
		if err := recover(); err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			applog.Warningf("panic serving %v: %v\n%s", c.remoteAddr, err, buf)
		}
		c.close() // FIXME: when to close the connection?
		applog.Debugf("Close connection from %s", c.remoteAddr)
	}()

	handler := c.server.Handler
	handler.Serve(c)
}

func (c *Conn) finalFlush() {
	if c.buf != nil {
		c.buf.Flush()
		putBufioReader(c.buf.Reader, c.bufswr)
		putBufioWriter(c.buf.Writer, c.bufsww)
		c.buf = nil
	}
}

func (c *Conn) close() {
	c.finalFlush()
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

type switchReader struct {
	io.Reader
}

type switchWriter struct {
	io.Writer
}

type bufioReaderPair struct {
	br *bufio.Reader
	sr *switchReader
}

type bufioWriterPair struct {
	bw *bufio.Writer
	sw *switchWriter
}

var (
	bufioReaderCache   = make(chan bufioReaderPair, 4)
	bufioWriterCache2k = make(chan bufioWriterPair, 4)
	bufioWriterCache4k = make(chan bufioWriterPair, 4)
)

func bufioWriterCache(size int) chan bufioWriterPair {
	switch size {
	case 2 << 10:
		return bufioWriterCache2k
	case 4 << 10:
		return bufioWriterCache4k
	}
	return nil
}

func newBufioReader(r io.Reader) (*bufio.Reader, *switchReader) {
	select {
	case p := <-bufioReaderCache:
		p.sr.Reader = r
		return p.br, p.sr
	default:
		sr := &switchReader{r}
		return bufio.NewReader(sr), sr
	}
}

func putBufioReader(br *bufio.Reader, sr *switchReader) {
	if n := br.Buffered(); n > 0 {
		io.CopyN(ioutil.Discard, br, int64(n))
	}
	br.Read(nil)
	sr.Reader = nil
	select {
	case bufioReaderCache <- bufioReaderPair{br, sr}:
	default:
	}
}

func newBufioWriterSize(w io.Writer, size int) (*bufio.Writer, *switchWriter) {
	select {
	case p := <-bufioWriterCache(size):
		p.sw.Writer = w
		return p.bw, p.sw
	default:
		sw := &switchWriter{w}
		return bufio.NewWriterSize(sw, size), sw
	}
}

func putBufioWriter(bw *bufio.Writer, sw *switchWriter) {
	if bw.Buffered() > 0 {
		return
	}
	if err := bw.Flush(); err != nil {
		return
	}
	sw.Writer = nil
	select {
	case bufioWriterCache(bw.Available()) <- bufioWriterPair{bw, sw}:
	default:
	}
}
