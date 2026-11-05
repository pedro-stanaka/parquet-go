package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func windowTestDictionary() *indexedType {
	d := ByteArrayType.NewDictionary(0, 256, ByteArrayType.NewValues(nil, nil))
	values := make([]Value, 256)
	for i := range values {
		values[i] = ByteArrayValue([]byte(fmt.Sprintf("label-%03d", i)))
	}
	d.Insert(make([]int32, len(values)), values)
	return newIndexedType(ByteArrayType, d)
}

func appendWindowRun(data []byte, count int, id uint32, width uint) []byte {
	data = binary.AppendUvarint(data, uint64(count)<<1)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], id)
	return append(data, b[:(width+7)/8]...)
}

func appendWindowPacked(data []byte, ids []int32, width uint) []byte {
	groups := (len(ids) + 7) / 8
	data = binary.AppendUvarint(data, uint64(groups)<<1|1)
	start := len(data)
	data = append(data, make([]byte, groups*int(width))...)
	for i, id := range ids {
		for bit := uint(0); bit < width; bit++ {
			if uint32(id)&(1<<bit) != 0 {
				pos := uint(i)*width + bit
				data[start+int(pos/8)] |= 1 << (pos % 8)
			}
		}
	}
	return data
}

func TestWindowedDictionaryCursor(t *testing.T) {
	typ := windowTestDictionary()
	for _, width := range []uint{0, 1, 2, 3, 7, 8, 9, 16, 31, 32} {
		for _, batch := range []int{1, 3, 7, 4095, 4096, 4097, 10000} {
			t.Run(fmt.Sprintf("width%d/batch%d", width, batch), func(t *testing.T) {
				mask := int32(255)
				if width < 8 {
					mask = int32(1<<width) - 1
				}
				ids := make([]int32, 9003)
				for i := range ids {
					ids[i] = int32(i*17) & mask
				}
				data := appendWindowRun([]byte{byte(width)}, 8193, uint32(mask), width)
				data = appendWindowPacked(data, ids, width)
				data = appendWindowRun(data, 4101, 0, width)
				data = data[:len(data):len(data)] // Exercise unpadded run tails.
				want := append(make([]int32, 8193), ids...)
				for i := 0; i < 8193; i++ {
					want[i] = mask
				}
				// Every packed group contributes eight IDs. These zero IDs between
				// runs are logical values, not surplus truncated at the page count.
				want = append(want, make([]int32, 5+4101)...)
				page, err := newWindowedIndexedPage(typ, 2, len(want), data)
				if err != nil {
					t.Fatal(err)
				}
				r := page.Values()
				values := make([]Value, batch)
				offset := 0
				for {
					n, err := r.ReadValues(values)
					for i, v := range values[:n] {
						if string(v.ByteArray()) != string(typ.dict.Index(want[offset+i]).ByteArray()) || v.Column() != 2 {
							t.Fatalf("value %d: %v", offset+i, v)
						}
					}
					offset += n
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if offset != len(want) {
					t.Fatal(offset)
				}
				p := page.(*windowedIndexedPage)
				if p.materialized != nil {
					t.Fatal("ReadValues eagerly materialized")
				}
				lo, hi, ok := page.Bounds()
				if !ok || lo.Column() != 2 || hi.Column() != 2 {
					t.Fatal("bounds")
				}
				if p.materialized != nil {
					t.Fatal("Bounds materialized")
				}
				for _, span := range [][2]int64{{0, 0}, {8192, 8200}, {4095, 17000}, {int64(len(want) - 3), int64(len(want))}} {
					slice := page.Slice(span[0], span[1])
					sliceData := slice.Data()
					if !reflect.DeepEqual(sliceData.Int32(), want[span[0]:span[1]]) {
						t.Fatal("slice parity")
					}
				}
				pageData := page.Data()
				if !reflect.DeepEqual(pageData.Int32(), want) {
					t.Fatal("Data parity")
				}
			})
		}
	}
}

func TestWindowedDictionaryValidation(t *testing.T) {
	typ := windowTestDictionary()
	tests := []struct {
		name  string
		data  []byte
		count int
		valid bool
	}{
		{"zero legacy", nil, 100, true}, {"zero width", []byte{0}, 100, true},
		{"width", []byte{33}, 1, false}, {"header truncated", []byte{1, 128}, 1, false},
		{"header overflow", append([]byte{1}, bytes.Repeat([]byte{255}, 11)...), 1, false},
		{"empty nonzero", []byte{1}, 1, true}, {"short run", []byte{1, 2, 0}, 2, true},
		{"run truncated", []byte{8, 2}, 1, false}, {"packed truncated", []byte{8, 3, 0}, 1, false},
		{"id out of range", appendWindowRun([]byte{16}, 1, 256, 16), 1, false},
		{"negative id", appendWindowRun([]byte{32}, 1, 0xffffffff, 32), 1, false},
		{"packed invalid id", appendWindowPacked([]byte{16}, []int32{256}, 16), 1, false},
		{"invalid padding ignored", appendWindowPacked([]byte{16}, []int32{1, 256}, 16), 1, true},
		{"zero count runs", []byte{1, 0, 0, 2, 1}, 1, true},
		{"truncated after count", []byte{1, 2, 0, 128}, 1, false},
		{"oversized run", appendWindowRun([]byte{1}, 1<<30, 0, 1), 7, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newWindowedIndexedPage(typ, 0, tt.count, tt.data)
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v, err=%v", tt.valid, err)
			}
		})
	}
}

func windowTestFile(t testing.TB, width, rows, pageSize, version int) []byte {
	t.Helper()
	group := Group{}
	for i := 0; i < width; i++ {
		group[fmt.Sprintf("label_%03d", i)] = Encoded(String(), &RLEDictionary)
	}
	group["plain"] = String()
	group["optional"] = Optional(Encoded(String(), &RLEDictionary))
	group["number"] = Encoded(Leaf(Int64Type), &RLEDictionary)
	group["repeated"] = Repeated(Encoded(String(), &RLEDictionary))
	schema := NewSchema("synthetic", group)
	var buf bytes.Buffer
	w := NewWriter(&buf, schema, PageBufferSize(pageSize), DataPageVersion(version), Compression(&Snappy), MaxRowsPerRowGroup(int64(rows/2+1)))
	batch := make([]Row, 17)
	for start := 0; start < rows; {
		n := min(len(batch), rows-start)
		for j := 0; j < n; j++ {
			row := make(Row, width+4)
			for col, path := range schema.Columns() {
				switch path[0] {
				case "number":
					row[col] = Int64Value(int64((start+j)%13)).Level(0, 0, col)
				case "repeated":
					row[col] = ByteArrayValue([]byte("repeated")).Level(0, 1, col)
				case "optional":
					if (start+j)%3 == 0 {
						row[col] = NullValue().Level(0, 0, col)
					} else {
						row[col] = ByteArrayValue([]byte("optional")).Level(0, 1, col)
					}
				default:
					row[col] = ByteArrayValue([]byte(fmt.Sprintf("value-%02d", (start+j)/41%13))).Level(0, 0, col)
				}
			}
			batch[j] = row
		}
		if _, err := w.WriteRows(batch[:n]); err != nil {
			t.Fatal(err)
		}
		start += n
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestWindowedDictionaryFileParity(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, pageSize := range []int{1024, 1 << 20} {
			t.Run(fmt.Sprintf("v%d/page%d", version, pageSize), func(t *testing.T) {
				data := windowTestFile(t, 32, 9001, pageSize, version)
				base, err := OpenFile(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				file, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true))
				if err != nil {
					t.Fatal(err)
				}
				// Verify the supported columns actually reach the lazy path, while
				// optional, plain and numeric columns retain the normal implementation.
				for i, chunk := range file.RowGroups()[0].ColumnChunks() {
					pages := chunk.Pages()
					p, err := pages.ReadPage()
					if err != nil {
						t.Fatal(err)
					}
					_, lazy := p.(*bufferedDictionaryPage)
					path := file.Schema().Columns()[i][0]
					expected := path != "plain" && path != "optional" && path != "number" && path != "repeated"
					if lazy != expected {
						t.Fatalf("%s lazy=%v", path, lazy)
					}
					Release(p)
					pages.Close()
				}
				a, b := NewReader(base), NewReader(file)
				defer a.Close()
				defer b.Close()
				arAll, brAll := make([]Row, 127), make([]Row, 127)
				total := 0
				for {
					an, ae := a.ReadRows(arAll)
					bn, be := b.ReadRows(brAll)
					if an != bn || ae != be || !reflect.DeepEqual(arAll[:an], brAll[:bn]) {
						t.Fatalf("full scan parity at %d", total)
					}
					total += bn
					if be == io.EOF {
						break
					}
					if be != nil {
						t.Fatal(be)
					}
				}
				if total != 9001 {
					t.Fatal(total)
				}
				var retained []Row
				for _, seek := range []int64{0, 4095, 8995, 33, 4500, 0} {
					if err := a.SeekToRow(seek); err != nil {
						t.Fatal(err)
					}
					if err := b.SeekToRow(seek); err != nil {
						t.Fatal(err)
					}
					ar, br := make([]Row, 7), make([]Row, 7)
					for k := 0; k < 9; k++ {
						an, ae := a.ReadRows(ar)
						bn, be := b.ReadRows(br)
						if an != bn || ae != be || !reflect.DeepEqual(ar[:an], br[:bn]) {
							t.Fatalf("parity at %d: %d/%d %v/%v", seek, an, bn, ae, be)
						}
						for _, r := range br[:bn] {
							retained = append(retained, r.Clone())
						}
						if be == io.EOF {
							break
						}
					}
				}
				// Non-cloned byte values also remain valid after Close and subsequent
				// readers recycle buffers (the usual Rows ownership guarantee).
				a.SeekToRow(0)
				b.SeekToRow(0)
				ar, br := make([]Row, 1), make([]Row, 1)
				a.ReadRows(ar)
				b.ReadRows(br)
				want := ar[0].Clone()
				held := append(Row(nil), br[0]...)
				b.Close()
				for k := 0; k < 3; k++ {
					r := NewReader(file)
					r.ReadRows(make([]Row, 50))
					r.Close()
				}
				if !reflect.DeepEqual(want, held) {
					t.Fatal("retained values changed")
				}
				if len(retained) == 0 {
					t.Fatal("no retained rows")
				}
			})
		}
	}
}

func TestWindowedDictionaryOwnershipAndCompatibility(t *testing.T) {
	data := windowTestFile(t, 4, 10001, 1<<20, 2)
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)), &FileConfig{WindowedDictionaryReading: true}, FileReadMode(ReadModeAsync))
	if err != nil {
		t.Fatal(err)
	}
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	// A sliced page owns an encoded-buffer reference independently of the
	// original page and the page iterator, even after closing that iterator.
	slice := page.Slice(4094, 4102)
	Release(page)
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	defer Release(slice)
	values := make([]Value, 8)
	if n, err := slice.Values().ReadValues(values); n != 8 || err != io.EOF {
		t.Fatalf("slice read %d: %v", n, err)
	}
	want := append([]Value(nil), values...)
	for i := range want {
		want[i] = want[i].Clone()
	}
	v := slice.Data()
	encoded, err := slice.Type().Encode(nil, v, &RLEDictionary)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := RLEDictionary.DecodeInt32(nil, encoded)
	if err != nil || !reflect.DeepEqual(decoded[:8], v.Int32()) {
		t.Fatalf("Data encoding: %v", err)
	}
	lo, hi, ok := slice.Bounds()
	if !ok || slice.Type().Compare(lo, hi) > 0 {
		t.Fatal("slice bounds")
	}
	for i := 0; i < 3; i++ {
		r := NewReader(f)
		r.ReadRows(make([]Row, 500))
		r.Close()
	}
	if !reflect.DeepEqual(want, values) {
		t.Fatal("slice values invalidated")
	}

	typ := windowTestDictionary()
	ids := make([]int32, 10001)
	for i := range ids {
		ids[i] = int32(i % 256)
	}
	encoded, err = RLEDictionary.EncodeInt32(nil, ids)
	if err != nil {
		t.Fatal(err)
	}
	p, err := newWindowedIndexedPage(typ, 0, len(ids), encoded)
	if err != nil {
		t.Fatal(err)
	}
	r := p.Values()
	held := make([]Value, 3)
	r.ReadValues(held)
	clone := Row(held).Clone()
	rest := make([]Value, 5001)
	for {
		_, err := r.ReadValues(rest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(Row(held), clone) {
		t.Fatal("overwritten window escaped into values")
	}
	if len(r.(*windowedIndexedPageValues).window) != dictionaryIndexWindowSize {
		t.Fatal("window size")
	}
	if p.(*windowedIndexedPage).materialized != nil {
		t.Fatal("eager index allocation")
	}
}

func TestWindowedDictionaryColumnWindowReuse(t *testing.T) {
	data := windowTestFile(t, 1, 10001, 1024, 2)
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true))
	if err != nil {
		t.Fatal(err)
	}
	r := NewColumnChunkValueReader(f.RowGroups()[0].ColumnChunks()[0]).(*columnChunkValueReader)
	values := make([]Value, 127)
	var first *int32
	var firstPage Page
	transitions := 0
	for {
		n, err := r.ReadValues(values)
		if n > 0 {
			if first == nil {
				first = &r.indexWindow[0]
				firstPage = r.page
			}
			if first != &r.indexWindow[0] {
				t.Fatal("window reallocated across pages")
			}
			if r.page != firstPage {
				transitions++
				firstPage = r.page
			}
			if r.values.(*windowedIndexedPageValues).page.materialized != nil {
				t.Fatal("eager column read")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if transitions == 0 {
		t.Fatal("no page transitions")
	}
	if err := r.SeekToRow(7); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadValues(values); err != nil {
		t.Fatal(err)
	}
	if first != &r.indexWindow[0] {
		t.Fatal("window reallocated on seek")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.indexWindow != nil {
		t.Fatal("window retained after close")
	}
}

func TestWindowedDictionaryDefaultOff(t *testing.T) {
	data := windowTestFile(t, 1, 32, 4096, 2)
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	p, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	defer Release(p)
	if _, ok := p.(*bufferedPage).Page.(*indexedPage); !ok {
		t.Fatalf("default page is %T", p.(*bufferedPage).Page)
	}
}

func BenchmarkWindowedDictionaryRead(b *testing.B) {
	typ := windowTestDictionary()
	for _, count := range []int{4096, 1 << 20} {
		for _, packed := range []bool{false, true} {
			ids := make([]int32, count)
			for i := range ids {
				if packed {
					ids[i] = int32(i % 256)
				} else {
					ids[i] = 1
				}
			}
			data, err := RLEDictionary.EncodeInt32(nil, ids)
			if err != nil {
				b.Fatal(err)
			}
			for _, lazy := range []bool{false, true} {
				b.Run(fmt.Sprintf("n%d/packed%v/lazy%v", count, packed, lazy), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(count * 4))
					values := make([]Value, 127)
					for i := 0; i < b.N; i++ {
						var p Page
						if lazy {
							p, err = newWindowedIndexedPage(typ, 0, count, data)
						} else {
							var decoded []int32
							decoded, err = RLEDictionary.DecodeInt32(nil, data)
							p = newIndexedPage(typ, makeColumnIndex(0), int32(count), encoding.Int32Values(decoded))
						}
						if err != nil {
							b.Fatal(err)
						}
						r := p.Values()
						for {
							_, err := r.ReadValues(values)
							if err == io.EOF {
								break
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkWindowedDictionaryWideFile(b *testing.B) {
	for _, pageSize := range []int{4096, 1 << 22} {
		data := windowTestFile(b, 64, 32769, pageSize, 2)
		for _, lazy := range []bool{false, true} {
			b.Run(fmt.Sprintf("page%d/lazy%v", pageSize, lazy), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(lazy))
					if err != nil {
						b.Fatal(err)
					}
					r := NewReader(f)
					rows := make([]Row, 127)
					for {
						_, err := r.ReadRows(rows)
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					r.Close()
				}
			})
		}
	}
}
