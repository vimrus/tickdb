package store

import (
	"bytes"
)

const (
	LevelYear   int8 = 0x01
	LevelMonth  int8 = 0x02
	LevelDay    int8 = 0x04
	LevelHour   int8 = 0x08
	LevelMinute int8 = 0x10
	LevelSecond int8 = 0x20
	LevelTime   int8 = 0x40
)

type node struct {
	level    int8
	pointers []*nodePointer // interior nodes will have this
	points   []*Point       // leaf nodes will have this
}

type nodePointer struct {
	key          []byte
	next         uint64
	pointer      uint64
	reducedValue []byte
}

func (np *nodePointer) encodeRoot() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw48(np.pointer))
	buf.Write(np.reducedValue)
	return buf.Bytes()
}

func (np *nodePointer) encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw48(np.pointer))
	buf.Write(encode_raw16(uint16(len(np.reducedValue))))
	buf.Write(np.reducedValue)
	return buf.Bytes()
}

func decodeRootNodePointer(data []byte) *nodePointer {
	np := &nodePointer{}
	np.pointer = decode_raw48(data[0:6])
	np.reducedValue = data[RootBaseSize:]
	return np
}
