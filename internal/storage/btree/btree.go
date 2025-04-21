package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// Node represents a single node in the B-Tree.
type Node struct {
	keys     []int         // Keys stored in the node.
	values   []interface{} // Corresponding values.
	children []*Node       // Children nodes (nil if leaf).
	isLeaf   bool          // Whether the node is a leaf.
	degree   int           // Minimum degree (defines the order of the tree).
	id       int32         // Unique identifier for the node.
}

func (n *Node) Keys() []int {
	return n.keys
}

func (n *Node) Values() []interface{} {
	return n.values
}

func (n *Node) Children() []*Node {
	return n.children
}

func (n *Node) IsLeaf() bool {
	return n.isLeaf
}

func (n *Node) Degree() int {
	return n.degree
}

func (n *Node) ID() int32 {
	return n.id
}

func NewNodeComplete(id int32, keys []int, values []interface{}, children []*Node, isLeaf bool, degree int) *Node {
	return &Node{
		keys:     keys,
		values:   values,
		children: children,
		isLeaf:   isLeaf,
		degree:   degree,
		id:       id,
	}
}

func NewNode(id int32, degree int) *Node {
	return &Node{
		id:     id,
		degree: degree,
	}
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

func (t *BTree) Root() *Node {
	return t.root
}

func (t *BTree) ChildrenNode() []*Node {
	return t.root.children
}

func (t *BTree) Degree() int {
	return t.degree
}

func (t *BTree) SetRoot(root *Node) {
	t.root = root
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

// Delete deletes a key from the B-Tree.
func (t *BTree) Delete(key int) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.delete(t.root, key)
}

// Serialize serializes the B-Tree to a byte slice.
func (t *BTree) Serialize() ([]byte, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.serializeNode(t.root)

}

// Deserialize deserializes a byte slice to reconstruct the B-Tree.
func Deserialize(data []byte, fetchPage func(int32) ([]byte, error)) (*BTree, error) {
	buffer := bytes.NewReader(data)

	var id int32
	var isLeaf bool
	var degree int32
	var numKeys int32

	if err := binary.Read(buffer, binary.LittleEndian, &id); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, binary.LittleEndian, &isLeaf); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, binary.LittleEndian, &degree); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, binary.LittleEndian, &numKeys); err != nil {
		return nil, err
	}

	keys := make([]int, numKeys)
	for i := 0; i < int(numKeys); i++ {
		var key int32
		if err := binary.Read(buffer, binary.LittleEndian, &key); err != nil {
			return nil, err
		}
		keys[i] = int(key)
	}

	values := make([]interface{}, numKeys)
	for i := 0; i < int(numKeys); i++ {
		var valLen int32
		if err := binary.Read(buffer, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}
		str := make([]byte, valLen)
		if _, err := buffer.Read(str); err != nil {
			return nil, err
		}
		values[i] = string(str)
	}

	var numChildren int32
	if err := binary.Read(buffer, binary.LittleEndian, &numChildren); err != nil {
		return nil, err
	}

	children := make([]*Node, 0, numChildren)
	for i := 0; i < int(numChildren); i++ {
		var childID int32
		if err := binary.Read(buffer, binary.LittleEndian, &childID); err != nil {
			return nil, err
		}

		childData, err := fetchPage(childID)
		if err != nil {
			return nil, err
		}

		childTree, err := Deserialize(childData, fetchPage)
		if err != nil {
			return nil, err
		}
		children = append(children, childTree.root)
	}

	tree := NewBTree(int(degree))
	tree.root = NewNodeComplete(id, keys, values, children, isLeaf, int(degree))
	return tree, nil
}

func (t *BTree) serializeNode(node *Node) ([]byte, error) {
	buffer := new(bytes.Buffer)

	if err := binary.Write(buffer, binary.LittleEndian, node.id); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.LittleEndian, node.isLeaf); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(t.degree)); err != nil {
		return nil, err
	}

	numKeys := int32(len(node.keys))
	if err := binary.Write(buffer, binary.LittleEndian, numKeys); err != nil {
		return nil, err
	}
	for _, key := range node.keys {
		if err := binary.Write(buffer, binary.LittleEndian, int32(key)); err != nil {
			return nil, err
		}
	}

	for _, value := range node.values {
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not string")
		}
		if err := binary.Write(buffer, binary.LittleEndian, int32(len(str))); err != nil {
			return nil, err
		}
		if _, err := buffer.WriteString(str); err != nil {
			return nil, err
		}
	}

	numChildren := int32(len(node.children))
	if err := binary.Write(buffer, binary.LittleEndian, numChildren); err != nil {
		return nil, err
	}
	for _, child := range node.children {
		if err := binary.Write(buffer, binary.LittleEndian, child.id); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func (t *BTree) delete(node *Node, key int) error {
	idx := 0

	// Find the key in the current node
	for idx < len(node.keys) && node.keys[idx] < key {
		idx++
	}

	if idx < len(node.keys) && node.keys[idx] == key { // Key found
		if node.isLeaf {
			// Case 1: The node is a leaf
			node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
			node.values = append(node.values[:idx], node.values[idx+1:]...)
		} else {
			t.deleteInternalNodeKey(node, key, idx)
		}
	} else { // Key not found in the current node
		if node.isLeaf {
			return fmt.Errorf("key not found")
		}

		if idx >= len(node.children) {
			return fmt.Errorf("invalid child index %d for node with %d children", idx, len(node.children))
		}

		// Ensure the child has enough keys
		if len(node.children[idx].keys) < t.degree {
			t.ensureChildHasEnoughKeys(node, idx)
		}

		t.delete(node.children[idx], key)
	}

	return nil
}

func (t *BTree) deleteInternalNodeKey(node *Node, key, idx int) {
	if len(node.children[idx].keys) >= t.degree {
		predecessor := t.getPredecessor(node, idx)
		node.keys[idx] = predecessor.key
		node.values[idx] = predecessor.value
		t.delete(node.children[idx], predecessor.key)
	} else if len(node.children[idx+1].keys) >= t.degree {
		successor := t.getSuccessor(node, idx)
		node.keys[idx] = successor.key
		node.values[idx] = successor.value
		t.delete(node.children[idx+1], successor.key)
	} else {
		t.merge(node, idx)
		t.delete(node.children[idx], key)
	}
}

func (t *BTree) borrowFromLeft(node *Node, idx int) {
	child := node.children[idx]
	sibling := node.children[idx-1]

	child.keys = append([]int{node.keys[idx-1]}, child.keys...)
	child.values = append([]interface{}{node.values[idx-1]}, child.values...)

	if !child.isLeaf {
		child.children = append([]*Node{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}

	node.keys[idx-1] = sibling.keys[len(sibling.keys)-1]
	node.values[idx-1] = sibling.values[len(sibling.values)-1]

	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	sibling.values = sibling.values[:len(sibling.values)-1]
}

func (t *BTree) borrowFromRight(node *Node, idx int) {
	child := node.children[idx]
	sibling := node.children[idx+1]

	child.keys = append(child.keys, node.keys[idx])
	child.values = append(child.values, node.values[idx])

	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}

	node.keys[idx] = sibling.keys[0]
	node.values[idx] = sibling.values[0]

	sibling.keys = sibling.keys[1:]
	sibling.values = sibling.values[1:]
}

func (t *BTree) getPredecessor(node *Node, idx int) struct {
	key   int
	value interface{}
} {
	current := node.children[idx]
	for !current.isLeaf {
		current = current.children[len(current.children)-1]
	}
	return struct {
		key   int
		value interface{}
	}{key: current.keys[len(current.keys)-1], value: current.values[len(current.values)-1]}
}

func (t *BTree) getSuccessor(node *Node, idx int) struct {
	key   int
	value interface{}
} {
	current := node.children[idx+1]
	for !current.isLeaf {
		current = current.children[0]
	}
	return struct {
		key   int
		value interface{}
	}{key: current.keys[0], value: current.values[0]}
}

func (t *BTree) ensureChildHasEnoughKeys(node *Node, idx int) {
	child := node.children[idx]

	// Special case: if this is the root and it has only one child
	if node == t.root && len(node.children) == 1 {
		// Merge the root with its only child
		t.root = child
		return
	}

	// Try to borrow from left sibling if it exists and has enough keys
	if idx > 0 && len(node.children[idx-1].keys) >= t.degree {
		t.borrowFromLeft(node, idx)
		return
	}

	// Try to borrow from right sibling if it exists and has enough keys
	if idx < len(node.children)-1 && len(node.children[idx+1].keys) >= t.degree {
		t.borrowFromRight(node, idx)
		return
	}

	// If we can't borrow, we need to merge
	// If we're at the first child, merge with the right sibling
	if idx == 0 {
		t.merge(node, 0)
	} else {
		// Otherwise, merge with the left sibling
		t.merge(node, idx-1)
	}
}

func (t *BTree) merge(parent *Node, idx int) {
	// Special case for root with single child
	if parent == t.root && len(parent.children) == 1 {
		t.root = parent.children[0]
		return
	}

	if idx < 0 || idx >= len(parent.children)-1 {
		panic(fmt.Sprintf("merge: invalid index %d for parent with %d children", idx, len(parent.children)))
	}

	left := parent.children[idx]
	right := parent.children[idx+1]

	// Merge keys and values from parent and right into left
	left.keys = append(left.keys, parent.keys[idx])
	left.keys = append(left.keys, right.keys...)
	left.values = append(left.values, parent.values[idx])
	left.values = append(left.values, right.values...)

	// If not leaf, merge children as well
	if !left.isLeaf {
		left.children = append(left.children, right.children...)
	}

	// Remove the key and child reference from parent
	parent.keys = append(parent.keys[:idx], parent.keys[idx+1:]...)
	parent.values = append(parent.values[:idx], parent.values[idx+1:]...)
	parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)

	// If root becomes empty after merging, make the merged node the new root
	if parent == t.root && len(parent.keys) == 0 {
		t.root = left
	}
}
