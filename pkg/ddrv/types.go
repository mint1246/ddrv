package ddrv

import "errors"

// ErrClosed is returned when a writer or reader is
// closed and caller is trying to read or write
var ErrClosed = errors.New("is closed")

// ErrAlreadyClosed is returned when the reader/writer is already closed
var ErrAlreadyClosed = errors.New("already closed")

// Chunk represents a Discord attachment URL and Size
type Chunk struct {
	URL   string `json:"url"`  // URL where the data is stored
	Size  int    `json:"size"` // Size of the data
	Start int64  // Start position of the data in the overall data sequence
	End   int64  // End position of the data in the overall data sequence
	MId   string `json:"mid"` // Chunk message id
	Ex    int    `json:"ex"`  // Chunk link expiry time
	Is    int    `json:"is"`  // Chunk link issued time
	Hm    string `json:"hm"`  // Chunk link signature
}

// Message represents a Discord message and contains attachments (files uploaded within the message).
type Message struct {
	Id          string  `json:"id"`
	Attachments []Chunk `json:"attachments"`
}
