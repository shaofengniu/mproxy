package main

import (
	"fmt"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

type CommandCode uint8

const (
	GET        = CommandCode(0x00)
	SET        = CommandCode(0x01)
	ADD        = CommandCode(0x02)
	REPLACE    = CommandCode(0x03)
	DELETE     = CommandCode(0x04)
	INCREMENT  = CommandCode(0x05)
	DECREMENT  = CommandCode(0x06)
	QUIT       = CommandCode(0x07)
	FLUSH      = CommandCode(0x08)
	GETQ       = CommandCode(0x09)
	NOOP       = CommandCode(0x0a)
	VERSION    = CommandCode(0x0b)
	GETK       = CommandCode(0x0c)
	GETKQ      = CommandCode(0x0d)
	APPEND     = CommandCode(0x0e)
	PREPEND    = CommandCode(0x0f)
	STAT       = CommandCode(0x10)
	SETQ       = CommandCode(0x11)
	ADDQ       = CommandCode(0x12)
	REPLACEQ   = CommandCode(0x13)
	DELETEQ    = CommandCode(0x14)
	INCREMENTQ = CommandCode(0x15)
	DECREMENTQ = CommandCode(0x16)
	QUITQ      = CommandCode(0x17)
	FLUSHQ     = CommandCode(0x18)
	APPENDQ    = CommandCode(0x19)
	PREPENDQ   = CommandCode(0x1a)
	UNKNOWN    = CommandCode(0xff)
)

type Status uint16

const (
	SUCCESS         = Status(0x00)
	KEY_ENOENT      = Status(0x01)
	KEY_EEXISTS     = Status(0x02)
	E2BIG           = Status(0x03)
	EINVAL          = Status(0x04)
	NOT_STORED      = Status(0x05)
	DELTA_BADVAL    = Status(0x06)
	NOT_MY_VBUCKET  = Status(0x07)
	AUTH_ERROR      = Status(0x20)
	AUTH_CONTINUE   = Status(0x21)
	UNKNOWN_COMMAND = Status(0x81)
	ENOMEM          = Status(0x82)
	NOT_SUPPORTED   = Status(0x83)
	EINTERNAL       = Status(0x84)
	EBUSY           = Status(0x85)
	ETMPFAIL        = Status(0x86)
)

// Number of bytes in a binary protocol header.
const HDR_LEN = 24

const MaxBodyLen = 1 * 1024 * 1024

// Mapping of CommandCode -> name of command (not exhaustive)
var CommandNames map[CommandCode]string

var StatusNames map[Status]string

func init() {
	CommandNames = make(map[CommandCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "No error"
	StatusNames[KEY_ENOENT] = "Key not found"
	StatusNames[KEY_EEXISTS] = "Key exists"
	StatusNames[E2BIG] = "Value too large"
	StatusNames[EINVAL] = "Invalid arguments"
	StatusNames[NOT_STORED] = "Item not stored"
	StatusNames[DELTA_BADVAL] = "Incr/Decr on non-numeric value"
	StatusNames[NOT_MY_VBUCKET] = ""
	StatusNames[AUTH_ERROR] = "Auth error"
	StatusNames[AUTH_CONTINUE] = "Auth continue"
	StatusNames[UNKNOWN_COMMAND] = "Unknown command"
	StatusNames[ENOMEM] = "No memory"
	StatusNames[NOT_SUPPORTED] = "Not supported"
	StatusNames[EINTERNAL] = "Internal error"
	StatusNames[EBUSY] = "Server busy"
	StatusNames[ETMPFAIL] = "Temporary failure"
}

// String an op code.
func (o CommandCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String an op code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}

// Return true if a command is a "quiet" command.
func (o CommandCode) IsQuiet() bool {
	switch o {
	case GETQ,
		GETKQ,
		SETQ,
		ADDQ,
		REPLACEQ,
		DELETEQ,
		INCREMENTQ,
		DECREMENTQ,
		QUITQ,
		FLUSHQ,
		APPENDQ,
		PREPENDQ:
		return true
	}
	return false
}
