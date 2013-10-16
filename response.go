package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"

	"git.jumbo.ws/go/tcgl/applog"
)

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")

	clientError  = []byte("CLIENT_ERROR ")
	serverError  = []byte("SERVER_ERROR ")
	commandError = []byte("ERROR\r\n")
)

// Response header:

//   Byte/     0       |       1       |       2       |       3       |
//      /              |               |               |               |
//     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//     +---------------+---------------+---------------+---------------+
//    0| Magic         | Opcode        | Key Length                    |
//     +---------------+---------------+---------------+---------------+
//    4| Extras length | Data type     | Status                        |
//     +---------------+---------------+---------------+---------------+
//    8| Total body length                                             |
//     +---------------+---------------+---------------+---------------+
//   12| Opaque                                                        |
//     +---------------+---------------+---------------+---------------+
//   16| CAS                                                           |
//     |                                                               |
//     +---------------+---------------+---------------+---------------+
//     Total 24 bytes

type response struct {
	opcode CommandCode
	key    []byte
	flags  int
	bytes  int
	cas    int
	data   io.Reader
	status Status

	hdrBytes [24]byte
	extras   [4]byte
}

func (r *response) init(opcode CommandCode) {
	r.opcode = opcode
	r.key = nil
	r.flags = 0
	r.bytes = 0
	r.cas = 0
	r.data = nil
	r.status = SUCCESS

	hdr := r.hdrBytes[:]
	hdr[0] = RES_MAGIC
	hdr[1] = byte(opcode)
	for i := 2; i < HDR_LEN; i++ {
		hdr[i] = 0
	}
}

type TextReader interface {
	io.Reader
	ReadSlice(delim byte) (line []byte, err error)
}

type verboseTextReader struct {
	TextReader
}

func (r *verboseTextReader) Read(p []byte) (n int, err error) {
	if n, err = r.TextReader.Read(p); err == nil {
		applog.Debugf("%q", p[:n])
	}
	return
}

func (r *verboseTextReader) ReadSlice(delim byte) (line []byte, err error) {
	if line, err = r.TextReader.ReadSlice(delim); err == nil {
		applog.Debugf("%q", line)
	}
	return
}

func (r *response) ReadFrom(from io.Reader) (err error) {
	var reader TextReader
	reader, ok := from.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReader(from)
	}
	if verbose == 0 {
		reader = &verboseTextReader{reader}
	}

	switch r.opcode {
	case GET, GETQ, GETK, GETKQ:
		err = r.readRetrieval(reader)
	case SET, SETQ, ADD, ADDQ:
		err = r.readStorage(reader)
	case DELETE, DELETEQ:
		err = r.readDeletion(reader)
	default:
		err = fmt.Errorf("Unsupported opcode %s", r.opcode)
	}

	if err != nil {
		err = fmt.Errorf("Failed to read response: %v", err)
	}
	return
}

func (r *response) tryReadError(line []byte) bool {
	applog.Debugf("%q", line)
	if bytes.HasPrefix(line, clientError) {
		r.status = EINVAL
		return true
	}
	if bytes.HasPrefix(line, serverError) {
		r.status = EINTERNAL
		return true
	}

	if bytes.Equal(line, commandError) {
		r.status = UNKNOWN_COMMAND
		return true
	}
	return false
}

func (r *response) readRetrieval(from TextReader) (err error) {
	line, err := from.ReadSlice('\n')
	if err != nil {
		return
	}

	if r.tryReadError(line) {
		return nil
	}

	if bytes.Equal(line, resultEnd) {
		r.status = KEY_ENOENT
		return nil
	}

	pattern := "VALUE %s %d %d\r\n"
	dest := []interface{}{&r.key, &r.flags, &r.bytes}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return fmt.Errorf("Unexpected get response: %q", line)
	}

	r.data = from
	return
}

func (r *response) readStorage(from TextReader) (err error) {
	line, err := from.ReadSlice('\n')
	if err != nil {
		return
	}

	if r.tryReadError(line) {
		return nil
	}

	switch {
	case bytes.Equal(line, resultStored):
		r.status = SUCCESS
	case bytes.Equal(line, resultNotStored):
		r.status = NOT_STORED
	case bytes.Equal(line, resultExists):
		r.status = KEY_EEXISTS
	case bytes.Equal(line, resultNotFound):
		r.status = KEY_ENOENT
	default:
		return fmt.Errorf("Unexpected set response: %q", line)
	}

	return
}

func (r *response) readDeletion(from TextReader) (err error) {
	line, err := from.ReadSlice('\n')
	if err != nil {
		return err
	}

	if r.tryReadError(line) {
		return nil
	}

	switch {
	case bytes.Equal(line, resultDeleted):
		r.status = SUCCESS
	case bytes.Equal(line, resultNotFound):
		r.status = KEY_ENOENT
	default:
		return fmt.Errorf("Unexpected delete response: %q", line)
	}
	return
}

type verboseBinaryWriter struct {
	io.Writer
}

func (r *verboseBinaryWriter) Write(p []byte) (n int, err error) {
	if n, err = r.Writer.Write(p); err == nil {
		applog.Debugf("\n%s", hex.Dump(p[:n]))
	}
	return
}

func (r *response) WriteTo(to io.Writer) (err error) {
	if verbose == 0 {
		to = &verboseBinaryWriter{to}
	}

	if r.status != SUCCESS {
		return r.writeError(to)
	}

	switch r.opcode {
	case GET, GETQ, GETK, GETKQ:
		err = r.writeRetrieval(to)
	case SET, SETQ, ADD, ADDQ:
		err = r.writeStorage(to)
	case DELETE, DELETEQ:
		err = r.writeDeletion(to)
	default:
		err = fmt.Errorf("Unsupported opcode %s", r.opcode)
	}
	if err != nil {
		err = fmt.Errorf("Failed to write response: %v", err)
	}
	return
}

func (r *response) writeError(to io.Writer) (err error) {
	hdr := r.hdrBytes[:]
	// Status
	binary.BigEndian.PutUint16(hdr[6:], uint16(r.status))
	_, err = to.Write(hdr)
	return err
}

func (r *response) writeRetrieval(to io.Writer) (err error) {
	hdr := r.hdrBytes[:]
	// Opcode
	hdr[1] = byte(r.opcode)
	if r.opcode == GETK || r.opcode == GETKQ {
		// Key length
		binary.BigEndian.PutUint16(hdr[2:], uint16(len(r.key)))
	}
	// Extra length
	hdr[4] = 4
	// Total body
	if r.opcode == GETK || r.opcode == GETKQ {
		binary.BigEndian.PutUint32(hdr[8:], uint32(len(r.key)+r.bytes+4))
	} else {
		binary.BigEndian.PutUint32(hdr[8:], uint32(r.bytes+4))
	}

	// Header
	if _, err = to.Write(hdr); err != nil {
		return
	}
	// Flags
	extras := r.extras[:]
	binary.BigEndian.PutUint32(extras, uint32(r.flags))
	if _, err = to.Write(extras); err != nil {
		return
	}
	// Key
	if r.opcode == GETK || r.opcode == GETKQ {
		if _, err = to.Write(r.key); err != nil {
			return
		}
	}
	// Value
	if _, err = io.CopyN(to, r.data, int64(r.bytes)); err != nil {
		return
	}
	// Discard the \r\n from reader
	if _, err = io.CopyN(ioutil.Discard, r.data, 2); err != nil {
		return
	}
	return
}

func (r *response) writeStorage(to io.Writer) (err error) {
	hdr := r.hdrBytes[:]
	binary.BigEndian.PutUint64(hdr[16:], uint64(r.cas))
	_, err = to.Write(hdr)
	return err
}

func (r *response) writeDeletion(to io.Writer) (err error) {
	hdr := r.hdrBytes[:]
	_, err = to.Write(hdr)
	return err
}
