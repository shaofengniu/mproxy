package main

import (
	"git.jumbo.ws/go/tcgl/applog"
)

type MemcacheHandler struct {
	client *Client
}

func NewMemcacheHandler(ss ServerSelector) *MemcacheHandler {
	return &MemcacheHandler{
		client: NewFromSelector(ss),
	}
}

func (h *MemcacheHandler) Serve(c *Conn) {
	remote, err := h.client.PickConn("")
	if err != nil {
		return
	}

	requests := make(chan CommandCode, 256)
	c1 := make(chan bool)
	c2 := make(chan bool)

	go h.serveRequest(c, remote, requests, c1)
	go h.serveResponse(remote, c, requests, c2)

	select {
	case <-c1:
	case <-c2:
	}
	applog.Debugf("Exit handler")
}

func (h *MemcacheHandler) serveRequest(from *Conn, to *conn, requests chan CommandCode, complete chan bool) (err error) {
	defer func() {
		applog.Debugf("Exit request loop: %v", err)
		complete <- true
		requests <- QUIT
	}()

	var req request
	for {
		if err = req.ReadFrom(from); err != nil {
			return
		}
		if err = req.WriteTo(to); err != nil {
			return
		}
		if err = to.Flush(); err != nil {
			return
		}

		requests <- req.opcode
	}
}

func (h *MemcacheHandler) serveResponse(from *conn, to *Conn, requests chan CommandCode, complete chan bool) (err error) {
	defer func() {
		applog.Debugf("Exit response loop: %v", err)
		complete <- true
	}()

	var rsp response
	for {
		select {
		case req := <-requests:
			if req == QUIT {
				return
			}
			rsp.init(req)
			if err = rsp.ReadFrom(from); err != nil {
				return
			}
			if err = rsp.WriteTo(to); err != nil {
				return
			}
			if err = to.Flush(); err != nil {
				return
			}
		}
	}
}
