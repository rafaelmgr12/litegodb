package btree

import (
	"sync"
)

// Node represents a single node in the B-Tree.
type Node struct {
	keys     []int         // Keys stored in the node.
	values   []interface{} // Corresponding values.
	children []*Node       // Children nodes (nil if leaf).
	isLeaf   bool          // Whether the node is a leaf.
	degree   int           // Minimum degree (defines the order of the tree).
}

// BTree represents the overall B-Tree.
type BTree struct {
	root   *Node      // Root node of the tree.
	degree int        // Minimum degree.
	mutex  sync.Mutex // Mutex for thread-safety
}

// NewBTree creates a new B-Tree with the specified degree.
func NewBTree(degree int) *BTree {
	if degree < 2 {
		degree = 2 // Ensure valid minimum degree
	}
	return &BTree{
		root: &Node{
			keys:     make([]int, 0, 2*degree-1),
			values:   make([]interface{}, 0, 2*degree-1),
			children: make([]*Node, 0, 2*degree),
			isLeaf:   true,
			degree:   degree,
		},
		degree: degree,
	}
}

// Insert inserts a key-value pair into the B-Tree.
func (t *BTree) Insert(key int, value interface{}) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if value == nil {
		panic("value cannot be nil")
	}
	if t.root == nil {
		t.root = &Node{
			keys:     make([]int, 0, 2*t.degree-1),
			values:   make([]interface{}, 0, 2*t.degree-1),
			children: make([]*Node, 0, 2*t.degree),
			isLeaf:   true,
			degree:   t.degree,
		}
	}
	root := t.root

	// If the root is full, create a new root
	if len(root.keys) == 2*t.degree-1 {
		newRoot := &Node{
			keys:     make([]int, 0, 2*t.degree-1),
			values:   make([]interface{}, 0, 2*t.degree-1),
			children: make([]*Node, 0, 2*t.degree),
			isLeaf:   false,
			degree:   t.degree,
		}
		t.root = newRoot
		newRoot.children = append(newRoot.children, root)
		t.splitChild(newRoot, 0)
		t.insertNonFull(newRoot, key, value)
	} else {
		t.insertNonFull(root, key, value)
	}
}

func (t *BTree) splitChild(parent *Node, childIndex int) {
	child := parent.children[childIndex]
	newChild := &Node{
		keys:     make([]int, 0, t.degree-1),
		values:   make([]interface{}, 0, t.degree-1),
		children: make([]*Node, 0, t.degree),
		isLeaf:   child.isLeaf,
		degree:   t.degree,
	}

	// Median index
	mid := t.degree - 1

	// Move half of the keys and values to the new node
	newChild.keys = append(newChild.keys, child.keys[mid+1:]...)
	newChild.values = append(newChild.values, child.values[mid+1:]...)

	// If not leaf, move the appropriate children
	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[mid+1:]...)
		child.children = child.children[:mid+1]
	}

	// Insert Key and Value in the parent
	insertAt := 0
	for insertAt < len(parent.keys) && parent.keys[insertAt] < child.keys[mid] {
		insertAt++
	}

	// Insert into the parent
	parent.keys = append(parent.keys, 0)
	parent.values = append(parent.values, nil)
	copy(parent.keys[insertAt+1:], parent.keys[insertAt:])
	copy(parent.values[insertAt+1:], parent.values[insertAt:])
	parent.keys[insertAt] = child.keys[mid]
	parent.values[insertAt] = child.values[mid]

	// Adjust the original child node
	parent.children = append(parent.children, nil)
	copy(parent.children[insertAt+2:], parent.children[insertAt+1:])
	parent.children[insertAt+1] = newChild

	// Trim the original child node
	child.keys = child.keys[:mid]
	child.values = child.values[:mid]
}

func (t *BTree) insertNonFull(node *Node, key int, value interface{}) {
	i := len(node.keys) - 1

	if node.isLeaf {

		// Check for duplicate keys and update the value if found
		for idx, k := range node.keys {
			if k == key {
				node.values[idx] = value
				return
			}
		}

		// find the correct position to insert the key
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++

		// insert the key and value
		node.keys = append(node.keys, 0)
		node.values = append(node.values, nil)
		if i < len(node.keys)-1 {
			copy(node.keys[i+1:], node.keys[i:])
			copy(node.values[i+1:], node.values[i:])
		}
		node.keys[i] = key
		node.values[i] = value
	} else {
		// find the right children
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++

		// if the children is full, split it
		if len(node.children[i].keys) == 2*t.degree-1 {
			t.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		t.insertNonFull(node.children[i], key, value)
	}
}

// Search searches for a key in the B-Tree and returns the value, if found.
func (t *BTree) Search(key int) (interface{}, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.searchNode(t.root, key)
}

func (t *BTree) searchNode(node *Node, key int) (interface{}, bool) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if i < len(node.keys) && key == node.keys[i] {
		return node.values[i], true
	}

	if node.isLeaf {
		return nil, false
	}

	return t.searchNode(node.children[i], key)
}
