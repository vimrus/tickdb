package storage

import (
	"hash/crc32"
)

const ChunkLengthSize int64 = 4
const ChunkCrcSize int64 = 4

// read a chunk at the specified location
func (db *DB) readChunkAt(pos int64) ([]byte, error) {
	// chunk starts with 8 bytes (32bit length, 32bit crc)
	chunkPrefix := make([]byte, ChunkLengthSize+ChunkCrcSize)
	n, err := db.ops.ReadAt(chunkPrefix, pos)
	if err != nil {
		return nil, err
	}

	size := decodeUint32(chunkPrefix[0:ChunkLengthSize])
	crc := decodeUint32(chunkPrefix[ChunkLengthSize : ChunkLengthSize+ChunkCrcSize])

	size -= uint32(ChunkLengthSize)
	data := make([]byte, size)
	pos += int64(n)
	n, err = db.ops.ReadAt(data, pos)
	if uint32(n) < size {
		return nil, ErrChunkDataLessThanSize
	}

	// validate crc
	actualCRC := crc32.ChecksumIEEE(data)
	if actualCRC != crc {
		return nil, ErrChunkBadCrc
	}
	return data, nil
}

// write chunk at the specified location, after the end as usually
func (db *DB) writeChunk(buf []byte) (int64, int64, error) {
	startPos := db.pos

	// chunk starts with 8 bytes (32bit length, 32bit crc)
	size := uint32(len(buf)) + uint32(ChunkCrcSize)

	sizeBytes := encodeUint32(size)
	written, err := db.ops.WriteAt(sizeBytes, db.pos)
	if err != nil {
		return db.pos, int64(written), err
	}
	db.pos += int64(written)

	crc := crc32.ChecksumIEEE(buf)
	crcBytes := encodeUint32(crc)
	written, err = db.ops.WriteAt(crcBytes, db.pos)
	if err != nil {
		return db.pos, int64(written), err
	}
	db.pos += int64(written)
	written, err = db.ops.WriteAt(buf, db.pos)
	if err != nil {
		return db.pos, int64(written), err
	}
	db.pos += int64(written)

	return startPos, db.pos - startPos, nil
}
