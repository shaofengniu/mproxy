package main

import (
	"io"
	"time"

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
		applog.Errorf("Failed to pick connection: %s", err)
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
		complete <- true
		requests <- QUIT
	}()

	var req request
	for {
		if err = req.ReadFrom(from); err != nil && err != io.EOF {
			applog.Errorf("Failed to read request: %s", err)
			return
		}
		to.extendDeadline()
		if err = req.WriteTo(to); err != nil {
			applog.Errorf("Failed to write request: %s", err)
			return
		}
		start := time.Now()
		if err = to.Flush(); err != nil {
			delta := time.Now().Sub(start)
			applog.Errorf("Failed to flush request after %v: %s", delta, err)
			return
		}

		requests <- req.opcode
	}
}

func (h *MemcacheHandler) serveResponse(from *conn, to *Conn, requests chan CommandCode, complete chan bool) (err error) {
	defer func() {
		if err != nil {
			applog.Errorf("Exit response loop: %v", err)
		}
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
			from.extendDeadline()
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
