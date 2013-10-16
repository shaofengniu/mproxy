package main

import (
	"log"
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
		log.Println(err)
		return
	}

	requests := make(chan CommandCode, 256)
	complete := make(chan bool, 2)

	go h.serveRequest(c, remote, requests, complete)
	go h.serveResponse(remote, c, requests, complete)

	<-complete
	<-complete
}

func (h *MemcacheHandler) serveRequest(from *Conn, to *conn, requests chan CommandCode, complete chan bool) (err error) {
	defer func() {
		// TODO: handle error
		complete <- true
	}()

	var req request
	for {
		if err = req.ReadFrom(from); err != nil {
			return
		}
		if err = req.WriteTo(to); err != nil {
			return
		}
		// TODO: optimize the flush policy?
		if err = to.Flush(); err != nil {
			return
		}

		requests <- req.opcode
	}
}

func (h *MemcacheHandler) serveResponse(from *conn, to *Conn, requests chan CommandCode, complete chan bool) (err error) {
	defer func() {
		complete <- true
	}()

	var rsp response
	for {
		select {
		case req := <-requests:
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
