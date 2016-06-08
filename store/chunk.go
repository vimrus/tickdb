package store

import (
	"hash/crc32"
)

const ChunkLengthSize int64 = 4
const ChunkCrcSize int64 = 4

func (db *DB) readChunkAt(pos int64, header bool) ([]byte, error) {
	// chunk starts with 8 bytes (32bit length, 32bit crc)
	chunkPrefix := make([]byte, ChunkLengthSize+ChunkCrcSize)
	n, err := db.readAt(chunkPrefix, pos)
	if err != nil {
		return nil, err
	}

	size := decode_raw31(chunkPrefix[0:ChunkLengthSize])
	crc := decode_raw32(chunkPrefix[ChunkLengthSize : ChunkLengthSize+ChunkCrcSize])

	if header {
		size -= uint32(ChunkLengthSize) // headers include the length of the hash, data does not
	}

	data := make([]byte, size)
	pos += n // skip the actual number of bytes read for the header (may be more than header size if we crossed a block boundary)
	n, err = db.readAt(data, pos)
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

func (db *DB) writeChunk(buf []byte, header bool) (int64, int64, error) {
	// always write to the end of the file
	startPos := db.pos
	pos := startPos
	endpos := pos

	// if we're writing a header, advance to the next block size boundary
	if header {
		if pos%BlockSize != 0 {
			pos += (BlockSize - (pos % BlockSize))
			db.pos += (BlockSize - (pos % BlockSize))
		}
	}

	// chunk starts with 8 bytes (32bit length, 32bit crc)
	size := uint32(len(buf))
	if header {
		size += uint32(ChunkCrcSize) // header chunks include the length of the hash
	}
	crc := crc32.ChecksumIEEE(buf)

	var sizeBytes []byte
	if header {
		sizeBytes = encode_raw32(size)
	} else {
		sizeBytes = encode_raw31_highestbiton(size)
	}
	crcBytes := encode_raw32(crc)
	written, err := db.writeAt(sizeBytes, pos, header)
	if err != nil {
		return pos, written, err
	}
	db.pos += written
	pos += written
	endpos += written
	written, err = db.writeAt(crcBytes, pos, header)
	if err != nil {
		return pos, written, err
	}
	db.pos += written
	pos += written
	endpos += written
	written, err = db.writeAt(buf, pos, header)
	if err != nil {
		return pos, written, err
	}
	db.pos += written
	endpos += written

	return startPos, endpos - startPos, nil
}
