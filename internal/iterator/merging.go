package iterator

import "github.com/xiatianliang1024gm/kvdb/internal/key"

// MergingIterator 把多个按 internal key 升序的迭代器归并成一个有序流。
//
// 子迭代器的顺序就是"新旧顺序"：越靠前的越新（MemTable → Immutable →
// 编号从大到小的 SST）。当两条记录的 internal key 完全相同时（同一个 seq 不该
// 出现在两个地方，但这里不做假设），先输出的那个来自更靠前的子迭代器。
//
// 用最小堆而不是"每次线性扫描找最小"：子迭代器的数量等于参与读取的文件数 + 2，
// 在 L0 文件变多时会明显增长，堆能把每次 Next 的代价压在 O(log k)。
type MergingIterator struct {
	cmp      key.InternalComparer
	children []Iterator

	items []int // 堆：当前有效的子迭代器下标，按各自的当前 key 排序
	err   error
}

// NewMerging 归并 children，children 顺序即新旧顺序。
func NewMerging(cmp key.InternalComparer, children ...Iterator) *MergingIterator {
	return &MergingIterator{cmp: cmp, children: children}
}

// less 定义堆序：先比 key，再比子迭代器下标（保证顺序确定）。
func (m *MergingIterator) less(i, j int) bool {
	ci, cj := m.children[m.items[i]], m.children[m.items[j]]
	if c := m.cmp.Compare(ci.Key(), cj.Key()); c != 0 {
		return c < 0
	}
	return m.items[i] < m.items[j]
}

func (m *MergingIterator) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !m.less(i, parent) {
			return
		}
		m.items[i], m.items[parent] = m.items[parent], m.items[i]
		i = parent
	}
}

func (m *MergingIterator) down(i int) {
	n := len(m.items)
	for {
		left, right := 2*i+1, 2*i+2
		smallest := i
		if left < n && m.less(left, smallest) {
			smallest = left
		}
		if right < n && m.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			return
		}
		m.items[i], m.items[smallest] = m.items[smallest], m.items[i]
		i = smallest
	}
}

// heapify 把 items 整理成堆。items 里的每个下标都已经定位到一条有效记录。
func (m *MergingIterator) heapify() {
	for i := len(m.items)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
}

// popRoot 弹出堆顶。
func (m *MergingIterator) popRoot() {
	n := len(m.items) - 1
	m.items[0] = m.items[n]
	m.items = m.items[:n]
	if n > 0 {
		m.down(0)
	}
}

// checkErr 记录子迭代器的错误（第一个生效），并把出错的子迭代器从堆里摘掉。
func (m *MergingIterator) checkErr(idx int, c Iterator) bool {
	if err := c.Error(); err != nil {
		if m.err == nil {
			m.err = err
		}
		for i, v := range m.items {
			if v == idx {
				n := len(m.items) - 1
				m.items[i] = m.items[n]
				m.items = m.items[:n]
				m.heapify()
				break
			}
		}
		return false
	}
	return true
}

// SeekToFirst 定位到每个子迭代器的第一条记录。
func (m *MergingIterator) SeekToFirst() {
	m.items = m.items[:0]
	for i, c := range m.children {
		c.SeekToFirst()
		if !m.checkErr(i, c) {
			continue
		}
		if c.Valid() {
			m.items = append(m.items, i)
		}
	}
	m.heapify()
}

// Seek 定位到第一个 key >= target 的记录。
func (m *MergingIterator) Seek(target []byte) {
	m.items = m.items[:0]
	for i, c := range m.children {
		c.Seek(target)
		if !m.checkErr(i, c) {
			continue
		}
		if c.Valid() {
			m.items = append(m.items, i)
		}
	}
	m.heapify()
}

// Valid 表示归并流上是否还有记录。
func (m *MergingIterator) Valid() bool { return m.err == nil && len(m.items) > 0 }

// Key 返回当前记录的 internal key。
func (m *MergingIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}
	return m.children[m.items[0]].Key()
}

// Value 返回当前记录的 value。
func (m *MergingIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}
	return m.children[m.items[0]].Value()
}

// Next 前进一条记录。
func (m *MergingIterator) Next() {
	if len(m.items) == 0 {
		return
	}
	idx := m.items[0]
	c := m.children[idx]
	c.Next()
	if !m.checkErr(idx, c) {
		return
	}
	if !c.Valid() {
		m.popRoot()
		return
	}
	// 只有堆顶的子迭代器动了，其余位置仍然满足堆序，向下调整即可。
	m.down(0)
}

// Error 返回首个错误。
func (m *MergingIterator) Error() error { return m.err }
