package dataprovider

import (
	"time"

	"github.com/forscht/ddrv/pkg/ns"
)

type File struct {
	ID     string        `json:"id"`
	Name   string        `json:"name" validate:"required,regex=^[A-Za-z0-9_][A-Za-z0-9_. -]*[A-Za-z0-9_]$"`
	Dir    bool          `json:"dir"`
	Size   int64         `json:"size,omitempty"`
	Parent ns.NullString `json:"parent,omitempty" validate:"required,uuid"`
	MTime  time.Time     `json:"mtime"`
}

type Node struct {
	URL  string
	Size int
	Iv   string
	MId  string
	Ex   int
	Is   int
	Hm   string
}
