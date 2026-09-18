package sst

import "fmt"

// Iterator 前向遍历一个 SSTable 的全部记录（internal key 升序）。
//
// 它按需从块缓存/文件读入数据块：顺着索引块往前走，块读进来之后在块内顺序扫描，
// 一块读完再取下一块。因此"顺序扫描整个文件"的 IO 次数正好是块数，
// 而不是 M1 那样"每读一条都要从头扫"。
//
// 与块缓冲一样，Key() / Value() 返回的切片在下一次移动之前有效，
// 且可能直接指向块缓存里的字节，调用方不得修改。
type Iterator struct {
	r   *Reader
	ii  *blockIter // 索引块上的游标，指向当前数据块
	bi  *blockIter // 当前数据块上的游标
	err error
}

// NewIterator 返回一个尚未定位的迭代器。
//
// 索引块在 Open 时就已完整校验过，所以这里不会失败；真正的 IO 错误会在
// 遍历过程中通过 Error() 暴露。
func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{r: r}
	if r.numBlocks == 0 {
		return it
	}
	ii, err := newBlockIter(r.index, r.icmp.Compare)
	if err != nil {
		it.err = fmt.Errorf("%s: %w", r.path, err)
		return it
	}
	it.ii = ii
	return it
}

// Valid 表示迭代器是否指向一条有效记录。
func (it *Iterator) Valid() bool { return it.err == nil && it.bi != nil && it.bi.Valid() }

// Key 返回当前记录的 internal key。
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.bi.Key()
}

// Value 返回当前记录的 value；墓碑为空切片。
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.bi.Value()
}

// Error 返回遍历过程中遇到的首个错误。
func (it *Iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.bi != nil {
		if err := it.bi.Error(); err != nil {
			return fmt.Errorf("%s: %w", it.r.path, err)
		}
	}
	if it.ii != nil {
		if err := it.ii.Error(); err != nil {
			return fmt.Errorf("%s: %w", it.r.path, err)
		}
	}
	return nil
}

// SeekToFirst 定位到文件里最小的记录。
func (it *Iterator) SeekToFirst() {
	it.bi = nil
	if it.err != nil || it.ii == nil {
		return
	}
	it.ii.SeekToFirst()
	for it.ii.Valid() {
		if !it.loadCurrentBlock() {
			return
		}
		it.bi.SeekToFirst()
		if it.bi.Valid() {
			return
		}
		it.ii.Next() // 空块（正常情况下不会出现）跳过
	}
	it.bi = nil
}

// Seek 定位到第一个 >= target 的记录；不存在时迭代器失效。
func (it *Iterator) Seek(target []byte) {
	it.bi = nil
	if it.err != nil || it.ii == nil {
		return
	}
	it.ii.Seek(target)
	if !it.ii.Valid() {
		return // target 比文件里所有记录都大
	}
	if !it.loadCurrentBlock() {
		return
	}
	it.bi.Seek(target)
	if it.bi.Valid() {
		return
	}
	// 走到这里说明索引选中的块里没有 >= target 的记录。索引项是"块内最大 key"，
	// 按理不会发生，但保留这条退化路径，让任何索引异常都只表现为"多走一块"，
	// 而不是静默地少读数据。
	it.advanceBlock()
}

// Next 前进一条记录。
func (it *Iterator) Next() {
	if it.err != nil || it.bi == nil {
		return
	}
	it.bi.Next()
	if it.bi.Valid() {
		return
	}
	it.advanceBlock()
}

// advanceBlock 移动到下一个数据块的第一条记录。
func (it *Iterator) advanceBlock() {
	it.bi = nil
	for {
		it.ii.Next()
		if !it.ii.Valid() {
			return
		}
		if !it.loadCurrentBlock() {
			return
		}
		it.bi.SeekToFirst()
		if it.bi.Valid() {
			return
		}
	}
}

// loadCurrentBlock 读入索引游标当前指向的数据块。
func (it *Iterator) loadCurrentBlock() bool {
	h, _, err := decodeBlockHandle(it.ii.Value())
	if err != nil {
		it.fail(fmt.Errorf("%s: %w", it.r.path, err))
		return false
	}
	block, err := it.r.readBlock(h)
	if err != nil {
		it.fail(err)
		return false
	}
	bi, err := newBlockIter(block, it.r.icmp.Compare)
	if err != nil {
		it.fail(fmt.Errorf("%s: %w", it.r.path, err))
		return false
	}
	it.bi = bi
	return true
}

// fail 记录首个错误并让迭代器失效。
func (it *Iterator) fail(err error) {
	if it.err == nil {
		it.err = err
	}
	it.bi = nil
}
