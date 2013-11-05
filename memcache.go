package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"git.jumbo.ws/go/tcgl/applog"
)

var (
	ErrCacheMiss    = errors.New("memcache: cache miss")
	ErrCASConflict  = errors.New("memcache: compare-and-swap conflit")
	ErrNotStored    = errors.New("memcache: item not stored")
	ErrServerError  = errors.New("memcache: server error")
	ErrNoStats      = errors.New("memcache: no statistics available")
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")
	ErrNoServers    = errors.New("memcache: no servers configured or available")
)

const DefaultTimeout = time.Duration(100) * time.Millisecond

const (
	buffered            = 8 // arbitrary buffered channel size, for readability
	maxIdleConnsPerAddr = 2 // TODO(bradfitz): make this configurable?
)

func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

type ServerSelector interface {
	SetServers(servers []string) error
	PickServer(key string) (net.Addr, error)
}

type ServerList struct {
	mu    sync.Mutex
	addrs []net.Addr
}

func (ss *ServerList) SetServers(servers []string) error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = addr
		} else {
			addr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = addr
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr
	return nil
}

func (ss *ServerList) PickServer(key string) (net.Addr, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}

	return ss.addrs[rand.Intn(len(ss.addrs))], nil
}

type Client struct {
	Timeout  time.Duration
	selector ServerSelector
	mu       sync.Mutex
	freeconn map[string][]*conn
}

func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= maxIdleConnsPerAddr {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) PickConn(key string) (*conn, error) {
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return nil, err
	}
	cn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	return cn, nil
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	type connError struct {
		cn  net.Conn
		err error
	}
	ch := make(chan connError)
	go func() {
		nc, err := net.Dial(addr.Network(), addr.String())
		ch <- connError{nc, err}
	}()
	select {
	case ce := <-ch:
		return ce.cn, ce.err
	case <-time.After(c.netTimeout()):
		// Too slow. Fall through.
	}
	// Close the conn if it does end up finally coming in
	go func() {
		ce := <-ch
		if ce.err == nil {
			ce.cn.Close()
		}
	}()
	return nil, &ConnectTimeoutError{addr}
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	cn.extendDeadline()
	return cn, nil
}

type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

func (c *conn) Read(p []byte) (n int, err error) {
	c.extendDeadline()
	n, err = c.rw.Reader.Read(p)
	applog.Debugf("\n%s", hex.Dump(p[:n]))
	return
}

func (c *conn) ReadSlice(delim byte) (line []byte, err error) {
	c.extendDeadline()
	line, err = c.rw.Reader.ReadSlice(delim)
	applog.Debugf("\n%s", hex.Dump(line))
	return
}

func (c *conn) Write(p []byte) (n int, err error) {
	c.extendDeadline()
	n, err = c.rw.Writer.Write(p)
	applog.Debugf("\n%s", hex.Dump(p[:n]))
	return
}

func (c *conn) WriteTo(w io.Writer) (int64, error) {
	c.extendDeadline()
	return c.rw.Reader.WriteTo(c.rw.Writer)
}

func (c *conn) Close() error {
	return c.nc.Close()
}

func (c *conn) Flush() error {
	c.extendDeadline()
	return c.rw.Flush()
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}
