package store

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"unsafe"
)

// Represents a marker value to indicate that a file is the DB.
const magic uint32 = 0xED0CDAED

// The data file format version.
const version = 1

type DB struct {
	path     string
	file     *os.File
	opened   bool
	meta0    *meta
	meta1    *meta
	pageSize int

	rwlock   sync.Mutex // Allows only one writer at a time.
	metalock sync.Mutex // Protects meta page access.

	dataref []byte // mmap'ed readonly, write throws SEGV
	data    *[maxMapSize]byte
	datasz  int

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}
}

func Open(path string, mode os.FileMode) (*DB, error) {
	db := &DB{opened: true}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// Read the first meta page to determine the page size.
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Mark the database as opened and return.
	return db, nil
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Set the page size to the OS page size.
	db.pageSize = os.Getpagesize()

	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.bucket = 3
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// Write the buffer to our data file.
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("db.meta(): invalid meta pages")
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

func (db *DB) Get(key []int64) []byte {
	k, v, flags := db.Cursor().seek(key)

	// Return nil if this is a bucket.
	if flags != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Insert data, key is unixnano.
func (db *DB) Put(key int64, value map[string]float64) error {
	t, err := db.beginRWTx()
	if err != nil {
		return err
	}

	// Move cursor to correct position.
	c := db.Cursor()
	c.seek(key)
	c.node().put(key, value)
	ErrCommit := t.Commit()
	if ErrCommit != nil {
		return ErrCommit
	}

	return nil
}

func (db *DB) Cursor() *Cursor {
	// Allocate and return a cursor.
	return &Cursor{
		db:    db,
		stack: make([]elemRef, 0),
	}
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	// Close file handles.
	if db.file != nil {
		// Unlock the file.
		if err := funlock(db); err != nil {
			log.Printf("bolt.Close(): funlock error: %s", err)
		}

		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (db *DB) pageNode(id pgid) (*page, *node) {
	// Check the node cache.
	if db.nodes != nil {
		if n := db.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	return db.page(id), nil
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	freelist pgid
	data     pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
