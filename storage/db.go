package storage

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
	root     *node      // root node in memory, need flush

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

	// Check db whether exists.
	if db.pos == 0 {
		// Write meta
		db.meta = newMeta()
		err = db.writeMeta(db.meta)
		if err != nil {
			return nil, err
		}

		// Write root
		root := db.newLeafNode()
		root.level = LevelRoot
		db.pos = int64(MetaSize)
		db.writeChunk(root.encode())

		db.root = root
	} else {
		// Read meta
		err = db.loadMeta()
		if err != nil {
			return nil, err
		}

		// Read root
		db.root, err = db.node(db.meta.root)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// node read a chunk in the given positon, return node object.
func (db *DB) node(pos int64) (*node, error) {
	nodeBytes, err := db.readChunkAt(pos)
	if err != nil {
		return nil, err
	}
	return db.decodeNode(nodeBytes)
}

func (db *DB) loadMeta() error {
	chunk, err := db.readChunkAt(0)
	if err != nil {
		return err
	}
	db.meta, err = newMetaFromBytes(chunk)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) writeMeta(m *meta) error {
	metaBytes := m.toBytes()

	pos := db.pos
	db.pos = 0
	_, _, err := db.writeChunk(metaBytes)
	db.pos = pos

	if err != nil {
		return err
	}
	return nil
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// Build a query
func (db *DB) Query(from int64, to int64, level uint16, reducer map[string]string) []*Point {
	c := db.Cursor()
	c.level = level
	c.reducer = reducer

	var result []*Point
	c.seek(from)
	for {
		points := c.points()
		result = append(result, points...)
		if c.next() {
			break
		}
	}

	return result
}

func (db *DB) Get(key int64) (*Point, error) {
	c := db.Cursor()
	c.level = LevelNSecond

	c.seek(key)
	point := c.point()
	if point.Timestamp == key {
		return point, nil
	}

	return nil, ErrNotFound
}

// put insert data, key is unixnano.
func (db *DB) Put(key int64, value map[string]float64) error {
	tm := NewTime(key)

	c := db.Cursor()
	c.stack = c.stack[:0]

	// Move cursor to correct position.
	c.fix(&tm, db.root)

	return c.node().put(&tm, value)
}

func (db *DB) Delete(from int64, to int64) {
	fromTime := NewTime(from)
	toTime := NewTime(to)

	empty := db.root.clean(&fromTime, &toTime)
	if empty {
		db.root.isLeaf = true
	}
}

func (db *DB) Cursor() *Cursor {
	// Allocate and return a cursor.
	return &Cursor{
		db:    db,
		stack: make([]elemRef, 0),
	}
}

func (db *DB) Flush() error {
	// Flush root, save to meta.
	db.meta.root = db.root.flush()
	err := db.writeMeta(db.meta)
	if err != nil {
		return err
	}

	err = db.ops.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() error {
	db.file = nil
	db.path = ""
	return nil
}

const (
	magic        uint64 = 0xEF5D2BCA
	Version      uint16 = 1
	MetaSize     uint64 = 512
	MetaBaseSize uint64 = 3
	RootBaseSize uint64 = 12
)

type meta struct {
	magic   uint64
	version uint16
	root    int64
}

func newMeta() *meta {
	m := &meta{}
	m.version = Version
	m.root = int64(MetaSize)
	return m
}

func newMetaFromBytes(data []byte) (*meta, error) {
	m := &meta{}

	m.magic = decodeUint64(data[:8])
	m.version = decodeUint16(data[8:10])
	m.root = decodeInt64(data[10:])

	return m, nil
}

func (m *meta) toBytes() []byte {
	buf := new(bytes.Buffer)

	buf.Write(encodeUint64(m.magic))
	buf.Write(encodeUint16(m.version))
	buf.Write(encodeInt64(m.root))

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
