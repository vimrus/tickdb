package store

// node represents an in-memory, deserialized page.
type node struct {
	db         *DB
	isLeaf     bool
	unbalanced bool
	spilled    bool
	key        int64
	pgid       pgid
	parent     *node
	children   []*node
	inodes     []inode
}

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	flags uint32
	pgid  pgid
	key   []byte
	value []byte
}
