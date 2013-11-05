package main

import (
	"encoding/hex"
	"io"

	"git.jumbo.ws/go/tcgl/applog"
)

type ReadWriter interface {
	io.ReadWriter
	Flush() error
	ReadSlice(delim byte) ([]byte, error)
}

type verboseReadWriter struct {
	io.ReadWriter
}

func NewVerboseReadWriter(r io.ReadWriter) *verboseReadWriter {
	return &verboseReadWriter{r}
}

func (rw *verboseReadWriter) Read(p []byte) (n int, err error) {
	if n, err = rw.ReadWriter.Read(p); err == nil {
		applog.Debugf("\n%s", hex.Dump(p[:n]))
	}
	return
}

func (rw *verboseReadWriter) Write(p []byte) (n int, err error) {
	if n, err = rw.ReadWriter.Write(p); err == nil {
		applog.Debugf("\n%s", hex.Dump(p[:n]))
	}
	return
}

func (rw *verboseReadWriter) ReadSlice(delim byte) (line []byte, err error) {
	if r, ok := rw.ReadWriter.(interface {
		ReadSlice(delim byte) ([]byte, error)
	}); ok {
		if line, err = r.ReadSlice(delim); err == nil {
			applog.Debugf("\n%s", hex.Dump(line))
		}
	} else {
		panic("Unsupported method: ReadSlice")
	}

	return
}

func (rw *verboseReadWriter) Flush() error {
	if fw, ok := rw.ReadWriter.(interface {
		Flush() error
	}); ok {
		return fw.Flush()
	}
	return nil
}
