package main

type MemcacheHandler struct {
	client *Client
}

func NewMemcacheHandler(ss ServerSelector) *MemcacheHandler {
	return &MemcacheHandler{
		client: NewFromSelector(ss),
	}
}

func (h *MemcacheHandler) Serve(c *Conn) {
	remote, err := h.client.pickConn("")
	if err != nil {
		log.Println(err)
		return
	}

	requests := make(chan CommandCode, 256)

	go h.serveRequest(c, remote, requests)
	go h.serveResponse(remote, c, requests)

	<-complete
	<-complete
}

func (h *MemcacheHandler) serveRequest(from *Conn, to *conn, requests chan CommandCode) (err error) {
	defer func() {
		// TODO: handle error
	}()

	var req request
	for {
		if _, err = req.ReadFrom(from); err != nil {
			return
		}
		if _, err = req.WriteTo(to); err != nil {
			return
		}
		// TODO: optimize the flush policy?
		if err = to.Flush(); err != nil {
			return
		}

		requests <- req.opcode
	}
}

func (h *MemcacheHandler) serveResponse(from *conn, to *Conn, requests chan CommandCode) {
	var rsp response
	for {
		select {
		case req := <-requests:
			rsp.init(req)
			if _, err = rsp.ReadFrom(from); err != nil {
				return
			}
			if _, err = rsp.WriteTo(to); err != nil {
				return
			}
			if err = to.Flush(); err != nil {
				return
			}
		}
	}
}
