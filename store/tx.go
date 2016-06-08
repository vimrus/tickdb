package store

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with them.

type Tx struct {
	writable bool
	db       *DB
	meta     *meta
}

// init initializes the transaction.
func (tx *Tx) init(db *DB) {
	tx.db = db

	// Copy the meta page since it can be changed by the writer.
	tx.meta = &meta{}
	db.meta.copy(tx.meta)
}

func (tx *Tx) put(key int64, value map[string]float64) error {
	return nil
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
func (tx *Tx) Commit() error {
	return nil
}
