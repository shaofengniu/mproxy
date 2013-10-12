package main

import ()

type Handler interface {
	Serve(c *Conn)
}

func main() {
	ss := new(ServerList)
	ss.SetServers("localhost:22122")
	handler := NewMemcacheHandler(ss)
	s := Server{
		Addr:    ":8080",
		Handler: handler,
	}
	s.ListenAndServe()
}
