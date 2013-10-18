package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"

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
	verbose    int
	local      string
	remotes    stringSlice
	cpuprofile string
	memprofile string
)

func init() {
	flag.IntVar(&verbose, "v", 1, "set verbosity level")
	flag.StringVar(&local, "l", ":8080", "set local address")
	flag.Var(&remotes, "r", "remote address")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to this file")
	flag.StringVar(&memprofile, "memprofile", "", "write mem profile to this file")
}

type Handler interface {
	Serve(c *Conn) error
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	applog.SetLevel(verbose)

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			applog.Criticalf("Failed to create cpuprofile: %s", err)
		}
		pprof.StartCPUProfile(f)
		defer func() {
			pprof.StopCPUProfile()
			applog.Infof("Write cpuprofile to %s", cpuprofile)
		}()
	}

	if memprofile != "" {
		defer func() {
			f, err := os.Create(memprofile)
			if err != nil {
				applog.Criticalf("Failed to create memprofile: %s", err)
			}
			pprof.WriteHeapProfile(f)
			f.Close()
			applog.Infof("Write memprofile to %s", memprofile)
		}()
	}

	applog.Infof("local: %q", local)
	applog.Infof("remotes: %q", remotes)

	ss := new(ServerList)
	ss.SetServers(remotes)
	handler := NewMemcacheHandler(ss)
	s := Server{
		Addr:    local,
		Handler: handler,
	}
	go s.ListenAndServe()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	for {
		s := <-c
		applog.Infof("Got signal: %s", s)
		return
	}
}
