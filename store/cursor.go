package store

import (
	"fmt"
)

// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
type Cursor struct {
	db    *DB
	stack []elemRef
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
func (c *Cursor) seek(seek int64) (key int64, value []byte, flags uint32) {
	_assert(c.db != nil, "tx closed")

	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]
	c.search(seek, c.db.meta().data)
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.index >= ref.count() {
		return 0, nil, 0
	}

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() (int64, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return 0, nil, 0
	}

	inode := &ref.node.inodes[ref.index]
	return inode.key, inode.value, inode.flags

}

// search recursively performs a binary search against a given page/node until it finds a given key.
func (c *Cursor) search(key int64, pgid pgid) {
	p, n := c.db.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}

	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	return
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// Start from root and traverse down the hierarchy.
	var n = c.stack[0].node
	if n == nil {
		n = c.db.nodes[c.stack[0].page.id]
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		fmt.Println(ref)
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given page/node.
type elemRef struct {
	page  *page
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
