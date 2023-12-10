package ddrv

import (
	"fmt"
	"io"
	"strconv"
	"time"
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
	if chunkSize > 25*1024*1024 || chunkSize < 0 {
		return nil, fmt.Errorf("invalid chunk size %d", chunkSize)
	}
	return &Driver{rest: NewRest(tokens, channels), chunkSize: chunkSize}, nil
}

// NewWriter creates a new ddrv.Writer instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
func (d *Driver) NewWriter(onChunk func(chunk Node)) io.WriteCloser {
	return NewWriter(onChunk, d.chunkSize, d.rest)
}

// NewNWriter creates a new ddrv.NWriter instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
// NWriter buffers bytes into memory and writes data to discord in parallel
func (d *Driver) NewNWriter(onChunk func(chunk Node)) io.WriteCloser {
	return NewNWriter(onChunk, d.chunkSize, d.rest)
}

// NewReader creates a new Reader instance that implements an io.ReaderCloser.
// This allows for reading large files from Discord that were split into small chunks.
func (d *Driver) NewReader(chunks []Node, pos int64) (io.ReadCloser, error) {
	return NewReader(chunks, pos, d.rest)
}

// UpdateNodes finds expired chunks and updates chunk signature in given chunks slice
func (d *Driver) UpdateNodes(chunks []*Node) error {
	currentTimestamp := int(time.Now().Unix())
	expired := make(map[int64]*Node)

	for i, chunk := range chunks {
		if currentTimestamp > chunk.Ex {
			expired[chunk.MId] = chunks[i]
		}
	}

	var messages []Message
	for mid, chunk := range expired {
		if currentTimestamp > chunk.Ex {
			cid := extractChannelId(chunk.URL)
			fmt.Println(cid)
			if err := d.rest.GetMessages(cid, mid-1, "after", &messages); err != nil {
				return err
			}
			for _, msg := range messages {
				id, _ := strconv.ParseInt(msg.Id, 10, 64)
				if updatedChunk, ok := expired[id]; ok {
					updatedChunk.URL, updatedChunk.Ex, updatedChunk.Is, updatedChunk.Hm = decodeAttachmentURL(msg.Attachments[0].URL)
				}
			}
		}
	}
	return nil
}
