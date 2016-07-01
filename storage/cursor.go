package storage

import (
	"sort"
)

type Cursor struct {
	db      *DB
	level   uint16
	reducer map[string]string
	stack   []elemRef
}

// fix position to insert data.
func (c *Cursor) fix(t *Time, n *node) error {
	e := elemRef{node: n}
	c.stack = append(c.stack, e)

	if e.isLeaf() {
		if t.Level()>>1 == n.level {
			return nil
		}
		n.expand()
	}

	if len(n.pointers) == 0 {
		return nil
	}
	ts := t.Timestamp(n.level << 1).UnixNano()
	index := sort.Search(len(n.pointers), func(i int) bool {
		return n.pointers[i].key >= ts
	})

	// If the inserted node is not equal dirty node, flush the dirty.
	// Only one dirty branch in the tree.
	if n.dirty != -1 && n.dirty != index {
		n.pointers[n.dirty].pointer.reduce()
		n.pointers[n.dirty].pos = n.pointers[n.dirty].pointer.flush()
	}

	// Cannot find the key == ts
	if index == len(n.pointers) || n.pointers[index].key != ts {
		return nil
	}

	// Continue to fix to the insert node
	child := n.pointers[index].pointer
	if child == nil {
		var err error
		child, err = c.db.node(n.pointers[index].pos)
		child.parent = n
		if err != nil {
			return err
		}
		n.pointers[index].pointer = child
		n.dirty = index
	}

	e = elemRef{node: child}
	c.stack = append(c.stack, e)
	return c.fix(t, child)
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
func (c *Cursor) seek(seek int64) *Point {
	_assert(c.db != nil, "tx closed")

	// Start from root and traverse to correct position.
	c.stack = c.stack[:0]
	t := NewTime(seek)
	c.search(&t, c.db.root)
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of node then return nil.
	if ref.index >= ref.count() {
		return nil
	}

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// next moves the cursor to next node and return it.
func (c *Cursor) next() *Point {
	ref := &c.stack[len(c.stack)-1]
	ref.index++
	if ref.count() == 0 || ref.index >= ref.count() {
		if len(c.stack) <= 1 {
			return nil
		}
		c.stack = c.stack[:len(c.stack)-1]
		ref = &c.stack[len(c.stack)-1]

		return c.next()
	}

	if !ref.node.isLeaf && ref.node.level < c.level>>1 {
		err := c.first()
		if err != nil {
			return nil
		}
	}
	return c.keyValue()
}

// find the first node equal the level.
func (c *Cursor) first() error {
	ref := &c.stack[len(c.stack)-1]
	var n *node
	if ref.node.pointers[ref.index].pointer == nil {
		var err error
		n, err = c.db.node(ref.node.pointers[ref.index].pos)
		if err != nil {
			return err
		}
	} else {
		n = ref.node.pointers[ref.index].pointer
	}
	if !n.isLeaf && n.level < c.level>>1 {
		e := elemRef{node: n}
		c.stack = append(c.stack, e)
		c.first()
	}
	return nil
}

// keyValue returns the key and value of the current cursor.
func (c *Cursor) keyValue() *Point {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil
	}

	if ref.isLeaf() {
		point := ref.node.points[ref.index]
		return point
	}

	pointer := ref.node.pointers[ref.index]

	value := make(map[string]float64)
	for k, v := range pointer.value {
		switch c.reducer[k] {
		case "sum":
			value[k] = v.sum
		case "max":
			value[k] = v.max
		case "min":
			value[k] = v.min
		case "first":
			value[k] = v.first
		case "last":
			value[k] = v.last
		case "count":
			value[k] = float64(v.count)
		case "avg":
			value[k] = v.sum / float64(v.count)
		}
	}
	return &Point{
		Timestamp: pointer.key,
		Value:     value,
	}
}

// search recursively performs a binary search against a given node until it finds a given key.
func (c *Cursor) search(t *Time, n *node) error {
	e := elemRef{node: n}
	c.stack = append(c.stack, e)

	if e.isLeaf() {
		c.searchLeaf(t)
	} else {
		c.searchInterior(t)
	}
	return nil
}

func (c *Cursor) searchInterior(t *Time) {
	e := &c.stack[len(c.stack)-1]
	n := e.node
	ts := t.Timestamp(n.level << 1).UnixNano()
	index := sort.Search(len(n.pointers), func(i int) bool {
		return n.pointers[i].key >= ts
	})
	e.index = index

	if n.level<<1 >= c.level {
		return
	}

	// Recursively search to the next page.
	if n.pointers[index].pointer == nil {
		var err error
		n.pointers[index].pointer, err = n.db.node(n.pointers[index].pos)
		if err != nil {
			return
		}
	}
	c.search(t, n.pointers[index].pointer)
}

func (c *Cursor) searchLeaf(t *Time) {
	e := &c.stack[len(c.stack)-1]
	n := e.node
	ts := t.Timestamp(n.level << 1).UnixNano()
	index := sort.Search(len(n.points), func(i int) bool {
		return n.points[i].Timestamp >= ts
	})
	e.index = index
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	ref := &c.stack[len(c.stack)-1]
	return ref.node
}

// elemRef represents a reference to an element on a given node.
type elemRef struct {
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf node.
func (r *elemRef) isLeaf() bool {
	return r.node.isLeaf
}

func (r *elemRef) count() int {
	if r.node.isLeaf {
		return len(r.node.points)
	}
	return len(r.node.pointers)
}
