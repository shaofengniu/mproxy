package main

import (
	"flag"
	"fmt"
	"runtime"

	"git.jumbo.ws/go/tcgl/applog"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%s", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var (
	verbose int = 1
	local   string
	remotes stringSlice
)

func init() {
	flag.IntVar(&verbose, "v", 1, "set verbosity level")
	flag.StringVar(&local, "l", ":8080", "set local address")
	flag.Var(&remotes, "r", "remote address")
}

type Handler interface {
	Serve(c *Conn)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	applog.SetLevel(verbose)
	applog.Infof("local: %q", local)
	applog.Infof("remotes: %q", remotes)

	ss := new(ServerList)
	ss.SetServers(remotes)
	handler := NewMemcacheHandler(ss)
	s := Server{
		Addr:    local,
		Handler: handler,
	}
	s.ListenAndServe()
}
