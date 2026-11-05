package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/parquet-go/bitpack"
	"github.com/parquet-go/parquet-go/encoding"
)

const dictionaryIndexWindowSize = 4096

// dictionaryIndexCursor pauses inside both RLE runs and bit-packed groups.
// It never expands a run or allocates storage proportional to its length.
type dictionaryIndexCursor struct {
	data      []byte
	width     uint
	remaining int64
	word      int32
	packed    []byte
	bit       uint64
}

func newDictionaryIndexCursor(data []byte) (dictionaryIndexCursor, error) {
	c := dictionaryIndexCursor{}
	if len(data) != 0 {
		c.width = uint(data[0])
		c.data = data[1:]
	}
	if c.width > 32 {
		return c, fmt.Errorf("invalid dictionary index bit width %d", c.width)
	}
	return c, nil
}

func (c *dictionaryIndexCursor) nextRun() error {
	for len(c.data) > 0 {
		u, n := binary.Uvarint(c.data)
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		if n < 0 {
			return fmt.Errorf("dictionary index run header overflow")
		}
		c.data = c.data[n:]
		count := u >> 1
		if count > math.MaxInt32 {
			return fmt.Errorf("dictionary index run too long: %d", count)
		}
		if count == 0 {
			continue
		}
		c.packed = nil
		c.bit = 0
		if u&1 != 0 {
			size := count * uint64(c.width)
			if size > uint64(len(c.data)) {
				return io.ErrUnexpectedEOF
			}
			c.packed = c.data[:int(size)]
			c.data = c.data[int(size):]
			c.remaining = int64(count * 8)
		} else {
			size := int((c.width + 7) / 8)
			if size > len(c.data) {
				return io.ErrUnexpectedEOF
			}
			var b [4]byte
			copy(b[:], c.data[:size])
			c.word = int32(binary.LittleEndian.Uint32(b[:]))
			c.data = c.data[size:]
			c.remaining = int64(count)
		}
		return nil
	}
	return io.EOF
}

func (c *dictionaryIndexCursor) consume(n int64) {
	if c.packed != nil {
		c.bit += uint64(n) * uint64(c.width)
	}
	c.remaining -= n
}

func (c *dictionaryIndexCursor) skip(n int64) error {
	for n > 0 {
		if c.remaining == 0 {
			if err := c.nextRun(); err != nil {
				// Eager indexed pages zero-extend successfully decoded short streams.
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		m := min(n, c.remaining)
		c.consume(m)
		n -= m
	}
	return nil
}

func (c *dictionaryIndexCursor) read(dst []int32) (int, error) {
	n := 0
	for n < len(dst) {
		if c.remaining == 0 {
			if err := c.nextRun(); err != nil {
				if err == io.EOF {
					clear(dst[n:])
					return len(dst), nil
				}
				return n, err
			}
		}
		m := int(min(int64(len(dst)-n), c.remaining))
		if c.packed == nil || c.width == 0 {
			for i := range dst[n : n+m] {
				dst[n+i] = c.word
			}
		} else {
			c.readPacked(dst[n : n+m])
		}
		c.consume(int64(m))
		n += m
	}
	return n, nil
}

// readPacked unpacks whole aligned groups directly. Only partial groups and
// the unpadded tail need fixed scratch, independent of the encoded run length.
func (c *dictionaryIndexCursor) readPacked(dst []int32) {
	index := c.bit / uint64(c.width)
	for len(dst) > 0 {
		group, offset := index/8, int(index%8)
		in := c.packed[int(group)*int(c.width):]
		if offset == 0 && cap(in) >= bitpack.PaddingInt32 {
			n := min(len(dst)/8, (cap(in)-bitpack.PaddingInt32)/int(c.width)) * 8
			if n > 0 {
				bitpack.Unpack(dst[:n], in[:cap(in)], c.width)
				dst = dst[n:]
				index += uint64(n)
				continue
			}
		}
		var packed [32 + bitpack.PaddingInt32]byte
		var ids [8]int32
		copy(packed[:], in[:int(c.width)])
		bitpack.Unpack(ids[:], packed[:], c.width)
		n := copy(dst, ids[offset:])
		dst = dst[n:]
		index += uint64(n)
	}
}

// Validate before publishing the page: Data and Bounds cannot return errors,
// and dictionary lookup must never be handed an invalid index. RLE validation
// is constant-time per run; bit-packed IDs are checked in fixed-size windows.
func validateDictionaryIndexes(data []byte, count int64, dict Dictionary) error {
	c, err := newDictionaryIndexCursor(data)
	if err != nil {
		return err
	}
	var window [dictionaryIndexWindowSize]int32
	left := count
	for {
		if err := c.nextRun(); err != nil {
			if err != io.EOF {
				return err
			}
			if left > 0 && dict.Len() == 0 {
				return fmt.Errorf("invalid synthesized dictionary index 0")
			}
			return nil
		}
		n := min(left, c.remaining)
		if c.packed == nil || c.width == 0 {
			if n > 0 && (c.word < 0 || int64(c.word) >= int64(dict.Len())) {
				return fmt.Errorf("invalid dictionary index %d", c.word)
			}
			left -= n
			c.consume(c.remaining)
		} else {
			// Width < 32 excludes negative int32 IDs. Still parse every run,
			// including surplus values, to preserve structural validation.
			if c.width < 32 && uint64(dict.Len()) >= uint64(1)<<c.width {
				left -= n
				c.consume(c.remaining)
				continue
			}
			for n > 0 {
				m := min(n, int64(len(window)))
				if _, err := c.read(window[:int(m)]); err != nil {
					return err
				}
				for _, id := range window[:int(m)] {
					if id < 0 || int64(id) >= int64(dict.Len()) {
						return fmt.Errorf("invalid dictionary index %d", id)
					}
				}
				n -= m
				left -= m
			}
			c.consume(c.remaining) // Last bit-packed group may have padding IDs.
		}
	}
}

// Encoded indexes are retained for lazy decoding, but returned byte values
// borrow the dictionary instead. Detach therefore releases encoded storage.
type bufferedDictionaryPage struct {
	Page
	encoded *buffer[byte]
}

func newBufferedDictionaryPage(page Page, encoded *buffer[byte]) *bufferedDictionaryPage {
	bufferRef(encoded)
	return &bufferedDictionaryPage{Page: page, encoded: encoded}
}

func (p *bufferedDictionaryPage) Slice(i, j int64) Page {
	return newBufferedDictionaryPage(p.Page.Slice(i, j), p.encoded)
}
func (p *bufferedDictionaryPage) Retain()                 { bufferRef(p.encoded) }
func (p *bufferedDictionaryPage) Release()                { bufferUnref(p.encoded) }
func (p *bufferedDictionaryPage) ReleaseAndDetachValues() { p.Release() }

type windowedIndexedPage struct {
	typ          *indexedType
	data         []byte
	start, count int64
	columnIndex  uint16
	once         sync.Once
	materialized []int32
}

func newWindowedIndexedPage(typ *indexedType, column int, count int, data []byte) (Page, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative dictionary value count")
	}
	if typ.dict == nil {
		return nil, fmt.Errorf("dictionary page is missing")
	}
	if err := validateDictionaryIndexes(data, int64(count), typ.dict); err != nil {
		return nil, err
	}
	return &windowedIndexedPage{typ: typ, data: data, count: int64(count), columnIndex: ^makeColumnIndex(column)}, nil
}

func (p *windowedIndexedPage) Type() Type               { return indexedPageType{p.typ} }
func (p *windowedIndexedPage) Column() int              { return int(^p.columnIndex) }
func (p *windowedIndexedPage) Dictionary() Dictionary   { return p.typ.dict }
func (p *windowedIndexedPage) NumRows() int64           { return p.count }
func (p *windowedIndexedPage) NumValues() int64         { return p.count }
func (p *windowedIndexedPage) NumNulls() int64          { return 0 }
func (p *windowedIndexedPage) Size() int64              { return 4 * p.count }
func (p *windowedIndexedPage) RepetitionLevels() []byte { return nil }
func (p *windowedIndexedPage) DefinitionLevels() []byte { return nil }
func (p *windowedIndexedPage) cursor() dictionaryIndexCursor {
	c, _ := newDictionaryIndexCursor(p.data)
	c.skip(p.start) // The complete stream was validated by the constructor.
	return c
}
func (p *windowedIndexedPage) Data() encoding.Values {
	p.once.Do(func() {
		p.materialized = make([]int32, int(p.count))
		c := p.cursor()
		c.read(p.materialized)
	})
	return encoding.Int32Values(p.materialized)
}
func (p *windowedIndexedPage) Slice(i, j int64) Page {
	if i < 0 || j < i || j > p.count {
		panic("dictionary page slice out of bounds")
	}
	return &windowedIndexedPage{typ: p.typ, data: p.data, start: p.start + i, count: j - i, columnIndex: p.columnIndex}
}
func (p *windowedIndexedPage) Bounds() (minValue, maxValue Value, ok bool) {
	c := p.cursor()
	var window [dictionaryIndexWindowSize]int32
	for left := p.count; left > 0; {
		n := min(left, int64(len(window)))
		c.read(window[:int(n)])
		lo, hi := p.typ.dict.Bounds(window[:int(n)])
		if !ok || p.typ.Compare(lo, minValue) < 0 {
			minValue = lo
		}
		if !ok || p.typ.Compare(hi, maxValue) > 0 {
			maxValue = hi
		}
		ok = true
		left -= n
	}
	if ok {
		minValue.columnIndex = p.columnIndex
		maxValue.columnIndex = p.columnIndex
	}
	return
}
func (p *windowedIndexedPage) Values() ValueReader {
	return &windowedIndexedPageValues{page: p, cursor: p.cursor(), left: p.count}
}

type windowedIndexedPageValues struct {
	page   *windowedIndexedPage
	cursor dictionaryIndexCursor
	left   int64
	window []int32
}

func (r *windowedIndexedPageValues) ReadValues(values []Value) (n int, err error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	if len(values) == 0 {
		return 0, nil
	}
	if r.window == nil {
		r.window = make([]int32, min(r.left, dictionaryIndexWindowSize))
	}
	for n < len(values) && r.left > 0 {
		m := min(len(values)-n, len(r.window), int(r.left))
		read, readErr := r.cursor.read(r.window[:m])
		if read != m || readErr != nil {
			if readErr == nil || readErr == io.EOF {
				readErr = io.ErrUnexpectedEOF
			}
			return n, readErr
		}
		r.page.typ.dict.Lookup(r.window[:m], values[n:n+m])
		for i := range values[n : n+m] {
			values[n+i].columnIndex = r.page.columnIndex
		}
		n += m
		r.left -= int64(m)
	}
	if r.left == 0 {
		err = io.EOF
	}
	return
}
