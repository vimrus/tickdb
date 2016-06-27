package storage

import (
	"encoding/binary"
	"math"
)

func encodeUint16(v uint16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, v)
	return bytes
}

func decodeUint16(bytes []byte) uint16 {
	return binary.BigEndian.Uint16(bytes)
}

func encodeInt16(v int16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(v))
	return bytes
}

func decodeInt16(bytes []byte) int16 {
	return int16(binary.BigEndian.Uint16(bytes))
}

func encodeUint32(v uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, v)
	return bytes
}

func decodeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

func encodeInt32(v int32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(v))
	return bytes
}

func decodeInt32(bytes []byte) int32 {
	return int32(binary.BigEndian.Uint32(bytes))
}

func encodeUint64(v uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, v)
	return bytes
}

func decodeUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

func encodeInt64(v int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	return bytes
}

func decodeInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes))
}

func encodeFloat32(v float32) []byte {
	bits := math.Float32bits(v)
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, bits)
	return bytes
}

func decodeFloat32(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func encodeFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func decodeFloat64(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}
