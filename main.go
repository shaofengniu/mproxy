package main

import (
	"flag"
	"runtime"

	"git.jumbo.ws/go/tcgl/applog"
)

var (
	verbose int
)

func init() {
	flag.IntVar(&verbose, "v", 1, "set verbosity level")
}

type Handler interface {
	Serve(c *Conn)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	applog.SetLevel(verbose)

	ss := new(ServerList)
	ss.SetServers("localhost:11211")
	handler := NewMemcacheHandler(ss)
	s := Server{
		Addr:    ":8080",
		Handler: handler,
	}
	s.ListenAndServe()
}
