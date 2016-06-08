package store

const BlockSize int64 = 4096
const BlockMarkerSize int64 = 1

const (
	gs_BLOCK_DATA    byte = 0
	gs_BLOCK_HEADER  byte = 1
	gs_BLOCK_INVALID byte = 0xff
)

// this is just like os.File.ReadAt() except that if your read
// crosses a block boundary, the block marker is removed
func (db *DB) readAt(buf []byte, pos int64) (int64, error) {
	bytesReadSoFar := int64(0)
	bytesSkipped := int64(0)
	numBytesToRead := int64(len(buf))
	readOffset := pos
	for numBytesToRead > 0 {
		var err error
		bytesTillNextBlock := BlockSize - (readOffset % BlockSize)
		if bytesTillNextBlock == BlockSize {
			readOffset++
			bytesTillNextBlock--
			bytesSkipped++
		}
		bytesToReadThisPass := bytesTillNextBlock
		if bytesToReadThisPass > numBytesToRead {
			bytesToReadThisPass = numBytesToRead
		}
		n, err := db.ops.ReadAt(buf[bytesReadSoFar:bytesReadSoFar+bytesToReadThisPass], readOffset)
		if err != nil {
			return -1, err
		}
		readOffset += int64(n)
		bytesReadSoFar += int64(n)
		numBytesToRead -= int64(n)
		if int64(n) < bytesToReadThisPass {
			return bytesReadSoFar, nil
		}
	}
	return bytesReadSoFar + bytesSkipped, nil
}

func (db *DB) writeAt(buf []byte, pos int64, header bool) (int64, error) {
	var err error
	var bufSize int64 = int64(len(buf))
	var writePos int64 = pos
	var bufPos int64
	var written int
	var blockRemain int64
	var blockPrefix byte = 0x00
	if header {
		blockPrefix = 0x01
	}

	for bufPos < bufSize {
		blockRemain = BlockSize - (writePos % BlockSize)
		if blockRemain > (bufSize - bufPos) {
			blockRemain = bufSize - bufPos
		}

		if writePos%BlockSize == 0 {
			written, err = db.ops.WriteAt([]byte{blockPrefix}, writePos)
			if err != nil {
				return int64(written), err
			}
			writePos += 1
			continue
		}

		written, err = db.ops.WriteAt(buf[bufPos:bufPos+blockRemain], writePos)
		if err != nil {
			return int64(written), err
		}
		bufPos += int64(written)
		writePos += int64(written)
	}

	return writePos - pos, nil
}
