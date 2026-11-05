package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

func TestWindowedDictionaryCleanShortParity(t *testing.T) {
	typ := windowTestDictionary()
	for _, width := range []uint{0, 1, 3, 8, 9, 16, 31, 32} {
		for _, prefix := range []string{"empty", "rle", "packed", "mixed"} {
			t.Run(fmt.Sprintf("width%d/%s", width, prefix), func(t *testing.T) {
				data := []byte{byte(width)}
				id := uint32(1)
				if width == 0 {
					id = 0
				}
				if prefix == "rle" || prefix == "mixed" {
					data = appendWindowRun(data, 3, id, width)
				}
				if prefix == "packed" || prefix == "mixed" {
					data = appendWindowPacked(data, []int32{int32(id), 0, int32(id)}, width)
				}
				decoded, err := RLEDictionary.DecodeInt32(nil, data)
				if err != nil {
					t.Fatal(err)
				}
				const count = 43
				eager := newIndexedPage(typ, makeColumnIndex(0), count, encoding.Int32Values(decoded))
				lazy, err := newWindowedIndexedPage(typ, 0, count, data)
				if err != nil {
					t.Fatal(err)
				}
				assertWindowPageParity(t, eager, lazy)
				assertWindowPageParity(t, eager.Slice(1, 42), lazy.Slice(1, 42))
				assertWindowPageParity(t, eager.Slice(40, 43), lazy.Slice(40, 43))
			})
		}
	}
	empty := newIndexedType(ByteArrayType, ByteArrayType.NewDictionary(0, 0, ByteArrayType.NewValues(nil, nil)))
	for _, width := range []byte{0, 8, 32} {
		if _, err := newWindowedIndexedPage(empty, 0, 1, []byte{width}); err == nil {
			t.Fatal("accepted synthesized zero without a dictionary entry")
		}
	}
}

func assertWindowPageParity(t *testing.T, want, got Page) {
	t.Helper()
	wr, gr := want.Values(), got.Values()
	wv, gv := make([]Value, 3), make([]Value, 3)
	for {
		wn, we := wr.ReadValues(wv)
		gn, ge := gr.ReadValues(gv)
		if wn != gn || we != ge || !reflect.DeepEqual(wv[:wn], gv[:gn]) {
			t.Fatalf("Values mismatch: %d/%v, %d/%v", wn, we, gn, ge)
		}
		if we == io.EOF {
			break
		}
	}
	wl, wh, wok := want.Bounds()
	gl, gh, gok := got.Bounds()
	if wok != gok || !reflect.DeepEqual(wl, gl) || !reflect.DeepEqual(wh, gh) {
		t.Fatal("Bounds mismatch")
	}
	wd, gd := want.Data(), got.Data()
	if !reflect.DeepEqual(wd.Int32(), gd.Int32()) {
		t.Fatal("Data mismatch")
	}
}

func TestWindowedDictionaryExportedSourceOwnership(t *testing.T) {
	typ := windowTestDictionary()
	c := &Column{typ: ByteArrayType, file: &File{config: &FileConfig{WindowedDictionaryReading: true}}}
	for _, version := range []int{1, 2} {
		for _, mutation := range []string{"id", "header", "payload"} {
			t.Run(fmt.Sprintf("v%d/%s", version, mutation), func(t *testing.T) {
				data := appendWindowRun([]byte{16}, 2, 1, 16)
				var p Page
				var err error
				if version == 1 {
					p, err = c.DecodeDataPageV1(DataPageHeaderV1{header: &format.DataPageHeader{NumValues: 2, Encoding: format.RLEDictionary}}, data, typ.dict)
				} else {
					p, err = c.DecodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{NumValues: 2, NumRows: 2, Encoding: format.RLEDictionary}}, data, typ.dict)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer Release(p)
				slice := p.Slice(1, 2)
				defer Release(slice)
				switch mutation {
				case "id":
					data[2], data[3] = 0, 1 // ID 256 is outside the dictionary.
				case "header":
					data[1] = 2 // Clean short run after validation.
				case "payload":
					clear(data) // Even the width and run framing may be reused.
				}
				want := newIndexedPage(typ, makeColumnIndex(c.Index()), 2, encoding.Int32Values([]int32{1, 1}))
				assertWindowPageParity(t, want, p)
				assertWindowPageParity(t, want.Slice(1, 2), slice)
			})
		}
	}
}

func TestWindowedDictionaryEncodedBufferDetach(t *testing.T) {
	typ := windowTestDictionary()
	data := appendWindowRun([]byte{8}, 10, 1, 8)
	buf := buffers.get(len(data))
	copy(buf.data.Slice(), data)
	c := &Column{typ: ByteArrayType, file: &File{config: &FileConfig{WindowedDictionaryReading: true}}}
	page, err := c.decodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{NumValues: 10, Encoding: format.RLEDictionary}}, buf, typ.dict, -1)
	if err != nil {
		t.Fatal(err)
	}
	p := page.(*bufferedDictionaryPage)
	if p.encoded != buf || &p.Page.(*windowedIndexedPage).data[0] != &buf.data.Slice()[0] {
		t.Fatal("owned file buffer was copied or classified as borrowed contents")
	}
	slice := p.Slice(1, 3)
	Retain(p) // Simulate a cache reference independent of the caller.
	buf.unref()
	values := make([]Value, 10)
	p.Values().ReadValues(values)
	want := Row(values).Clone()
	releaseAndDetachValues(p)
	if buf.refc.Load() != 2 {
		t.Fatal("detach did not release encoded storage normally")
	}
	Release(p)
	assertWindowPageParity(t, newIndexedPage(typ, makeColumnIndex(c.Index()), 2, encoding.Int32Values([]int32{1, 1})), slice)
	releaseAndDetachValues(slice)
	if buf.refc.Load() != 0 {
		t.Fatal("slice abandoned encoded storage")
	}
	if !reflect.DeepEqual(Row(values), want) {
		t.Fatal("detaching encoded bytes invalidated borrowed dictionary values")
	}
}

func TestWindowedDictionaryConcurrentReaders(t *testing.T) {
	typ := windowTestDictionary()
	ids := make([]int32, 9003)
	for i := range ids {
		ids[i] = int32(i % 256)
	}
	p, err := newWindowedIndexedPage(typ, 0, len(ids), appendWindowPacked([]byte{9}, ids, 9))
	if err != nil {
		t.Fatal(err)
	}
	want := newIndexedPage(typ, makeColumnIndex(0), int32(len(ids)), encoding.Int32Values(ids))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assertWindowPageParity(t, want, p)
			assertWindowPageParity(t, want.Slice(3, 8001), p.Slice(3, 8001))
		}()
	}
	wg.Wait()
}

func TestWindowedDictionaryTrailingStructure(t *testing.T) {
	for _, width := range []uint{0, 1, 8, 16, 32} {
		prefix := appendWindowRun([]byte{byte(width)}, 1, 0, width)
		tails := [][]byte{{128}, bytes.Repeat([]byte{255}, 11)}
		if width > 0 {
			tails = append(tails, []byte{2}, []byte{3})
		}
		for _, tail := range tails {
			if _, err := newWindowedIndexedPage(windowTestDictionary(), 0, 1, append(append([]byte(nil), prefix...), tail...)); err == nil {
				t.Fatalf("accepted truncated/overflowing surplus run at width %d", width)
			}
		}
	}
}

func TestWindowedDictionaryRetainedRows(t *testing.T) {
	data := windowTestFile(t, 3, 10001, 1024, 2)
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true), FileReadMode(ReadModeAsync))
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(f)
	var held, want []Row
	rows := make([]Row, 127)
	for {
		n, err := r.ReadRows(rows)
		if n > 0 {
			// Copy only the Value structs, not their borrowed byte contents.
			held = append(held, append(Row(nil), rows[0]...))
			want = append(want, rows[0].Clone())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := r.SeekToRow(41); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadRows(rows); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Exercise new allocations/reuse after the original reader has closed.
	other := NewReader(f)
	for {
		_, err := other.ReadRows(rows)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(held, want) {
		t.Fatal("borrowed row contents changed across pages, row groups, seek or close")
	}
}

func TestWindowedDictionaryReadChecksCursorError(t *testing.T) {
	p, err := newWindowedIndexedPage(windowTestDictionary(), 0, 1, []byte{8})
	if err != nil {
		t.Fatal(err)
	}
	r := p.Values().(*windowedIndexedPageValues)
	r.cursor.data = []byte{128} // Break the internal validation invariant deliberately.
	values := []Value{ByteArrayValue([]byte("unchanged"))}
	if n, err := r.ReadValues(values); n != 0 || err != io.ErrUnexpectedEOF || string(values[0].ByteArray()) != "unchanged" {
		t.Fatalf("used stale window after cursor error: %d %v", n, err)
	}
}
