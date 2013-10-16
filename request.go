package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Request header:

//      Byte/     0       |       1       |       2       |       3       |
//         /              |               |               |               |
//        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//        +---------------+---------------+---------------+---------------+
//       0| Magic         | Opcode        | Key length                    |
//        +---------------+---------------+---------------+---------------+
//       4| Extras length | Data type     | Reserved                      |
//        +---------------+---------------+---------------+---------------+
//       8| Total body length                                             |
//        +---------------+---------------+---------------+---------------+
//      12| Opaque                                                        |
//        +---------------+---------------+---------------+---------------+
//      16| CAS                                                           |
//        |                                                               |
//        +---------------+---------------+---------------+---------------+
//        Total 24 bytes

type request struct {
	opcode   CommandCode
	keyLen   int
	extraLen int
	reserved int
	bodyLen  int
	opaque   uint32
	cas      uint64
	body     io.Reader
	hdrBytes [24]byte
	tmp      [4]byte
}

func (r *request) ReadFrom(from io.Reader) (err error) {
	hdr := r.hdrBytes[:]
	if _, err = io.ReadFull(from, hdr); err != nil {
		return
	}

	if hdr[0] != REQ_MAGIC {
		return fmt.Errorf("Bad magic: 0x%02x", hdr[0])
	}

	r.opcode = CommandCode(hdr[1])
	r.keyLen = int(binary.BigEndian.Uint16(hdr[2:]))
	r.extraLen = int(hdr[4])
	r.reserved = int(binary.BigEndian.Uint32(hdr[6:]))
	r.bodyLen = int(binary.BigEndian.Uint32(hdr[8:]))
	r.opaque = binary.BigEndian.Uint32(hdr[12:])
	r.cas = binary.BigEndian.Uint64(hdr[16:])

	if (r.bodyLen - r.keyLen - r.extraLen) > MaxBodyLen {
		return fmt.Errorf("BodyLen %d is too big (max %d)", r.bodyLen, MaxBodyLen)
	}

	r.body = from

	return nil
}

// Storage commands
// ----------------
// First, the client sends a command line which looks like this:

// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n

// After this line, the client sends the data block:
// <data block>\r\n

// Retrieval command:
// ------------------

// The retrieval commands "get" and "gets" operates like this:

// get <key>*\r\n
// gets <key>*\r\n

// Deletion
// --------

// The command "delete" allows for explicit deletion of items:

// delete <key> [noreply]\r\n
func (r *request) WriteTo(to io.Writer) (err error) {
	switch r.opcode {
	case GET, GETQ, GETK, GETKQ:
		return r.writeRetrieval(to)
	case SET, SETQ, ADD, ADDQ:
		return r.writeStorage(to)
	case DELETE, DELETEQ:
		return r.writeDeletion(to)
	default:
		return fmt.Errorf("Unsupported opcode %s", r.opcode)
	}
}

func (r *request) writeRetrieval(to io.Writer) (err error) {
	if _, err = fmt.Fprintf(to, "%s ", CommandNames[r.opcode]); err != nil {
		return
	}
	if _, err = io.CopyN(to, r.body, int64(r.keyLen)); err != nil {
		return
	}
	if _, err = to.Write(crlf); err != nil {
		return
	}
	return
}

func (r *request) writeStorage(to io.Writer) (err error) {
	// Read extra from request body
	if r.extraLen != 8 {
		return fmt.Errorf("Extra length %d is too small", r.extraLen)
	}
	tmp := r.tmp[:]
	if _, err = io.ReadFull(r.body, tmp); err != nil {
		return
	}
	flags := int(binary.BigEndian.Uint32(tmp))
	if _, err = io.ReadFull(r.body, tmp); err != nil {
		return
	}
	expire := int(binary.BigEndian.Uint32(tmp))

	if _, err = fmt.Fprintf(to, "%s ", CommandNames[r.opcode]); err != nil {
		return
	}
	// Write key
	if _, err = io.CopyN(to, r.body, int64(r.keyLen)); err != nil {
		return
	}
	// Write flags expire valuelen
	// FIXME: noreply
	vlen := r.bodyLen - r.extraLen - r.keyLen
	if _, err = fmt.Fprintf(to, " %d %d %d\r\n", flags, expire, vlen); err != nil {
		return
	}
	// Write value
	if _, err = io.CopyN(to, r.body, int64(vlen)); err != nil {
		return
	}
	if _, err = to.Write(crlf); err != nil {
		return
	}
	return
}

func (r *request) writeDeletion(to io.Writer) (err error) {
	if _, err = fmt.Fprintf(to, "%s ", CommandNames[r.opcode]); err != nil {
		return
	}
	if _, err = io.CopyN(to, r.body, int64(r.keyLen)); err != nil {
		return
	}
	if _, err = to.Write(crlf); err != nil {
		return
	}
	return
}
