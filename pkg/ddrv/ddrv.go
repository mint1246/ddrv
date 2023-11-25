package ddrv

import (
	"fmt"
	"io"
)

type Driver struct {
	rest      *Rest
	chunkSize int
}

func New(tokens []string, channels []string, chunkSize int) (*Driver, error) {
	if len(tokens) == 0 || len(channels) == 0 {
		return nil,
			fmt.Errorf("not enough tokens or channels : tokens %d channels %d", len(tokens), len(channels))
	}
	return &Driver{rest: NewRest(tokens, channels, false), chunkSize: chunkSize},
		nil
}

// NewWriter creates a new ddrv.Writer instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
func (d *Driver) NewWriter(onChunk func(chunk *Chunk)) io.WriteCloser {
	return NewWriter(onChunk, d.chunkSize, d.rest)
}

// NewNWriter creates a new ddrv.NWriter instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
// NWriter buffers bytes into memory and writes data to discord in parallel
func (d *Driver) NewNWriter(onChunk func(chunk *Chunk)) io.WriteCloser {
	return NewNWriter(onChunk, d.chunkSize, d.rest)
}

// NewReader creates a new Reader instance that implements an io.ReaderCloser.
// This allows for reading large files from Discord that were split into small chunks.
func (d *Driver) NewReader(chunks []Chunk, pos int64) (io.ReadCloser, error) {
	return NewReader(chunks, pos, d.rest)
}
