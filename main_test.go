package main

import (
	"flag"
	"fmt"
	"runtime"
	"testing"

	"git.jumbo.ws/go/tcgl/applog"
	"github.com/bmizerany/mc"
)

var (
	server string
	proxy  string
	num    int
)

func init() {
	flag.StringVar(&server, "server", "", "set server address")
	flag.StringVar(&proxy, "proxy", "", "set proxy address")
	flag.IntVar(&num, "num", 1, "number of concurrent connections")
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	applog.SetLevel(verbose)
}

type TB interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

func TestSet(t *testing.T) {
	initEnv()
	addr := newProxy(t)
	sc := newConn(t, server)
	pc := newConn(t, addr)

	err := sc.Del("foo")
	if err != nil && err != mc.ErrNotFound {
		t.Error(err)
	}

	err = pc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		t.Error(err)
	}

	val, _, _, err := sc.Get("foo")
	if err != nil {
		t.Error(err)
	}

	if val != "bar" {
		err = fmt.Errorf("result not match: %s", val)
		t.Error(err)
	}
}

func BenchmarkSet(b *testing.B) {
	addr := newProxy(b)
	conn := newConn(b, addr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Set("foo", "bar", 0, 0, 0)
	}
}

func TestGet(t *testing.T) {
	addr := newProxy(t)
	sc := newConn(t, server)
	pc := newConn(t, addr)

	err := sc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		t.Error(err)
	}

	val, _, _, err := pc.Get("foo")
	if err != nil {
		t.Error(err)
	}

	if val != "bar" {
		t.Error(err)
	}
}

func BenchmarkGet(b *testing.B) {
	addr := newProxy(b)
	sc := newConn(b, server)
	pc := newConn(b, addr)
	errors := 0
	err := sc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		b.Error(err)
	}

	done := make(chan bool, num)
	b.ResetTimer()

	for i := 0; i < num; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				val, _, _, err := pc.Get("foo")
				if err != nil || val != "bar" {
					errors += 1
					b.Error(err)
				}
			}
			done <- true
		}()

	}
	for i := 0; i < num; i++ {
		<-done
	}
	b.Logf("Get errors: %d", errors)
}

func TestDelete(t *testing.T) {
	addr := newProxy(t)
	sc := newConn(t, server)
	pc := newConn(t, addr)

	err := sc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		err = fmt.Errorf("asf")
		t.Error(err)
	}

	err = pc.Del("foo")
	if err != nil && err != mc.ErrNotFound {
		err = fmt.Errorf("asf")
		t.Error(err)
	}

	_, _, _, err = sc.Get("foo")
	if err != mc.ErrNotFound {
		err = fmt.Errorf("failed to delete foo")
		t.Error(err)
	}
}

func BenchmarkDelete(b *testing.B) {
	addr := newProxy(b)
	conn := newConn(b, addr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Del("foo")
	}
}

func newProxy(tb TB) string {
	if proxy != "" {
		return proxy
	}
	ss := new(ServerList)
	ss.SetServers([]string{server})
	handler := NewMemcacheHandler(ss)
	s := Server{
		Addr:    ":0",
		Handler: handler,
	}
	l, err := s.listen()
	if err != nil {
		tb.Errorf("Failed to start proxy: %s", err)
		return ""
	}
	go s.serve(l)
	return l.Addr().String()
}

func newConn(tb TB, addr string) *mc.Conn {
	conn, err := mc.Dial("tcp", addr)
	if err != nil {
		tb.Errorf("Failed connect to %s: %s", addr, err)
		return nil
	}
	return conn
}
