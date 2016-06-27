package storage

import (
	"bytes"
	"encoding/binary"
)

type Point struct {
	Timestamp int64              `json:timestamp`
	Value     map[string]float64 `json:value`
}

func (p *Point) encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encodeInt64(p.Timestamp))
	for k, v := range p.Value {
		keyBytes := []byte(k)
		buf.Write(encodeUint16(uint16(len(keyBytes))))
		buf.Write(keyBytes)
		buf.Write(encodeFloat64(v))
	}
	return buf.Bytes()
}

func newPoint() *Point {
	return &Point{
		Value: make(map[string]float64, 0),
	}
}

func decodePoint(pointBytes []byte) (*Point, error) {
	p := newPoint()
	p.Timestamp = int64(binary.BigEndian.Uint64(pointBytes[0:8]))
	bufPos := 8
	for bufPos < len(pointBytes) {
		keyLength := int(decodeUint16(pointBytes[bufPos : bufPos+2]))
		bufPos += 2
		key := string(pointBytes[bufPos : bufPos+keyLength])
		bufPos += keyLength
		value := decodeFloat64(pointBytes[bufPos : bufPos+8])
		bufPos += 8
		p.Value[key] = value
	}
	return p, nil
}
