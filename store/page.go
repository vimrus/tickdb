package store

import (
	"unsafe"
)

type pgid uint64

type page struct {
	id       pgid
	flags    uint16
	count    uint16
	overflow uint32
	ptr      uintptr
}

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}
