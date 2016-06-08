package store

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

type DB struct {
	path     string
	file     *os.File
	meta     *meta
	pos      int64
	metalock sync.Mutex // Allows only one writer at a time.
	rwlock   sync.Mutex // Allows only one writer at a time.
	rwtx     *Tx
	txs      []*Tx

	ops Ops
}

func Open(path string) (*DB, error) {
	db := &DB{path: path}

	var err error
	if db.file, err = db.ops.OpenFile(db.path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		_ = db.Close()
		return nil, err
	}

	db.pos, err = db.ops.GotoEOF()
	if err != nil {
		return nil, err
	}

	if db.pos == 0 {
		db.meta = newMeta()
		err = db.writeMeta(db.meta)
		if err != nil {
			return nil, err
		}
	} else {
		err = db.loadMeta()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *DB) loadMeta() error {
	chunk, err := db.readChunkAt(0, true)
	if err != nil {
		return err
	}
	m, err := newMetaFromBytes(chunk)
	if err != nil {
		return err
	}
	db.meta = m
	return nil
}

func (db *DB) writeMeta(m *meta) error {
	metaBytes := m.toBytes()
	_, _, err := db.writeChunk(metaBytes, true)
	if err != nil {
		return err
	}
	return nil
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

func (db *DB) Get(key int64) ([]byte, error) {
	return nil, nil
}

// Insert data, key is unixnano.
func (db *DB) Put(key int64, value map[string]float64) error {
	tx, err := db.beginRWTx()
	if err != nil {
		return err
	}

	err = tx.put(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t
	return t, nil
}

func (db *DB) Close() error {
	db.file = nil
	db.path = ""
	return nil
}

const (
	magic        uint64 = 0xEF5D2BCA
	Version      uint64 = 1
	MetaSize     uint64 = 512
	MetaBaseSize uint64 = 3
	RootBaseSize uint64 = 12
)

type meta struct {
	magic   uint64
	version uint64
	root    *nodePointer
}

func newMeta() *meta {
	m := &meta{}
	m.version = Version
	return m
}

func newMetaFromBytes(data []byte) (*meta, error) {
	m := &meta{}

	m.magic = uint64(decode_raw08(data[0:1]))
	m.version = uint64(decode_raw08(data[1:2]))
	rootSize := uint64(decode_raw16(data[2:3]))

	pointerOffset := int(MetaBaseSize)
	if rootSize > 0 {
		m.root = decodeRootNodePointer(data[pointerOffset : pointerOffset+int(rootSize)])
	}

	return m, nil
}

func (m *meta) toBytes() []byte {
	buf := new(bytes.Buffer)

	buf.Write(encode_raw08(m.magic))
	buf.Write(encode_raw08(m.version))

	var rootBytes []byte

	if m.root != nil {
		rootBytes = m.root.encodeRoot()
	}

	buf.Write(encode_raw16(uint16(len(rootBytes))))

	// fill to MetaSize
	buf.Write(make([]byte, int(MetaSize)-len(buf.Bytes())))

	return buf.Bytes()
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != Version {
		return ErrVersionMismatch
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// Quick operations for database file.
type Ops struct {
	File *os.File
}

func (o *Ops) OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	var err error
	o.File, err = os.OpenFile(path, flag, perm)
	return o.File, err
}

func (o *Ops) ReadAt(b []byte, off int64) (n int, err error) {
	return o.File.ReadAt(b, off)
}

func (o *Ops) WriteAt(b []byte, off int64) (n int, err error) {
	return o.File.WriteAt(b, off)
}

func (o *Ops) GotoEOF() (ret int64, err error) {
	return o.File.Seek(0, os.SEEK_END)
}

func (o *Ops) Sync() error {
	return o.File.Sync()
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
