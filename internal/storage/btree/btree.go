package btree

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
	root   *Node // Root node of the tree.
	degree int   // Minimum degree.
}

// NewBTree creates a new B-Tree with the specified degree.
func NewBTree(degree int) *BTree {
	if degree < 2 {
		degree = 2 // Garantir grau mínimo válido
	}
	return &BTree{
		root: &Node{
			keys:     make([]int, 0),
			values:   make([]interface{}, 0),
			children: make([]*Node, 0),
			isLeaf:   true,
			degree:   degree,
		},
		degree: degree,
	}
}

func (t *BTree) Insert(key int, value interface{}) {
	root := t.root

	// Se a raiz estiver cheia, criar nova raiz
	if len(root.keys) == 2*t.degree-1 {
		newRoot := &Node{
			keys:     make([]int, 0),
			values:   make([]interface{}, 0),
			children: make([]*Node, 0),
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

	// Índice mediano
	mid := t.degree - 1

	// Mover metade das chaves e valores para o novo nó
	newChild.keys = append(newChild.keys, child.keys[mid+1:]...)
	newChild.values = append(newChild.values, child.values[mid+1:]...)

	// Se não for folha, mover os filhos apropriados
	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[mid+1:]...)
		child.children = child.children[:mid+1]
	}

	// Inserir a chave mediana no pai
	insertAt := 0
	for insertAt < len(parent.keys) && parent.keys[insertAt] < child.keys[mid] {
		insertAt++
	}

	// Inserir no pai
	parent.keys = append(parent.keys, 0)
	parent.values = append(parent.values, nil)
	copy(parent.keys[insertAt+1:], parent.keys[insertAt:])
	copy(parent.values[insertAt+1:], parent.values[insertAt:])
	parent.keys[insertAt] = child.keys[mid]
	parent.values[insertAt] = child.values[mid]

	// Ajustar os filhos do pai
	parent.children = append(parent.children, nil)
	copy(parent.children[insertAt+2:], parent.children[insertAt+1:])
	parent.children[insertAt+1] = newChild

	// Truncar o nó filho original
	child.keys = child.keys[:mid]
	child.values = child.values[:mid]
}

func (t *BTree) insertNonFull(node *Node, key int, value interface{}) {
	i := len(node.keys) - 1

	if node.isLeaf {
		// Encontrar posição de inserção
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++

		// Inserir a chave e o valor
		node.keys = append(node.keys, 0)
		node.values = append(node.values, nil)
		if i < len(node.keys)-1 {
			copy(node.keys[i+1:], node.keys[i:])
			copy(node.values[i+1:], node.values[i:])
		}
		node.keys[i] = key
		node.values[i] = value
	} else {
		// Encontrar o filho apropriado
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++

		// Se o filho estiver cheio, dividir primeiro
		if len(node.children[i].keys) == 2*t.degree-1 {
			t.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		t.insertNonFull(node.children[i], key, value)
	}
}

func (t *BTree) Search(key int) (interface{}, bool) {
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
