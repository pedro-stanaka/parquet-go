# Opt-in windowed dictionary reading

```go
file, err := parquet.OpenFile(source, size, parquet.WindowedDictionaryReading(true))
// Handle err before using file.
reader := parquet.NewReader(file)
defer reader.Close()
```

The option defaults to false and also exists as
`FileConfig.WindowedDictionaryReading`. Enable it only on readers that need the
memory/CPU tradeoff, not on every file opened by an application.

## Supported path

Flat, required BYTE_ARRAY columns encoded with RLE_DICTIONARY or the legacy
PLAIN_DICTIONARY data-page encoding use a lazy indexed page. This includes
required string columns. Both data-page versions and compression are supported.
Optional, repeated, numeric, fixed-length and non-dictionary pages keep their
existing eager decoding behavior.

The lazy page retains the encoded page buffer and dictionary. Its cursor pauses
inside RLE runs and bit-packed groups. Repeated (RLE) runs, and the
zero-extended tail after a clean EOF, are never expanded: the dictionary value
is looked up once and copied across the output, so they use no index storage.
Bit-packed IDs are decoded into a window of at most 1024 int32 slots (4 KiB)
per value reader, sized to the caller's read batch (rounded up to eight) and
allocated only when a bit-packed run is first read. The row readers behind
ReadRows read 170 values per column, so their windows are 176 slots. A column
reader owns and reuses one window across pages and seeks, releasing it on
Close. Independent Page.Values readers use independent windows. Byte-array
values are written directly from dictionary offsets and reference dictionary
contents, never mutable index windows; they are identical to the eager path's
values. Existing page reference counting and row byte-value lifetime rules
remain in effect.

This is the same bounded intermediate-ID technique used by Arrow Rust
(`DictIndexDecoder`, 1024 IDs), Arrow C++ (`GetBatchWithDict`, 1024 IDs, with
direct repeated-run fills) and Polars. It does not stream the dictionary
itself.

Before returning a page, the encoded stream is structurally validated and
logical IDs are checked against the dictionary. RLE runs are validated without
expansion; bit-packed IDs are checked in fixed windows with an unsigned
max-reduction unless the dictionary covers every ID representable at that
width. Every run is still structurally
checked, including runs after the logical count. Surplus RLE values and packed
IDs are ignored, matching the eager decode-then-truncate policy; this is not
strict final-group padding validation. A clean EOF zero-extends short streams
at any width, also matching eager indexed pages. Synthesized zeros require a
nonempty dictionary. Incomplete run headers or payloads and invalid logical IDs
return errors rather than being passed to dictionary lookup.

## Compatibility and limits

- ReadValues and ReadRows do not materialize the full index array.
- Slice retains a lazy view; seeking does not require full decoding. Starting
  a slice replays preceding run headers, so repeated increasing-offset slices
  can become quadratic in run count; there are no cursor checkpoints yet.
- Bounds scans with bounded scratch space rather than materializing indexes;
  each RLE run contributes one entry.
- **Page.Data is an explicit exception:** it materializes the complete int32
  index array once per page/view, preserving the existing immutable Data and
  Type.Encode contract. It is safe for concurrent calls under normal page
  ownership rules. Requesting Data removes the bounded-index guarantee for
  that page. Row.Clone and Value.Clone retain their existing behavior.
- Encoded/decompressed pages, dictionaries, other columns, retained output
  values and compatibility materializations are separate memory costs. This
  option does not bound total heap, RSS, file size or reader fan-in.
- Exported Column.DecodeDataPageV1/V2 copy encoded input only for the opt-in
  eligible path, before validation, so subsequent caller reuse cannot change
  the lazy page. FilePages retains its owned buffers without copying. Encoded
  buffers return to their ordinary pool on release/detach; dictionary byte-value
  lifetime safeguards are unchanged.
- Packed decoding uses bounded bitpack.Unpack blocks, with stack scratch for
  partial groups and unpadded tails. There are no allocations per refill.
  Validation still unpacks bit-packed IDs a second time when the dictionary
  does not cover the bit width.

## Synthetic qualification

Commands (Go on darwin/arm64, Apple M3 Pro):

```sh
gofmt -w column.go column_chunk.go config.go dictionary_window*.go
go test ./...
go test -race ./...
go test -run '^$' -bench '^BenchmarkWindowedDictionary(Read|WideFile)$' \
  -benchmem -benchtime=20x -count=8
```

Full tests and race tests pass. Tests cover run/window/page boundaries, varied
bit widths, short repeated reads, padding, malformed/truncated encodings,
invalid IDs, clean-short eager differential cases at widths 0/1/3/8/9/16/31/32,
caller input mutation after exported V1/V2 decoding, encoded-buffer reference
counts through detach/cache/slices, actual concurrent Data/Values/Bounds
readers, lazy slices, seeks, asynchronous page ownership, uncloned retained
row contents across row groups and close, clone/encode compatibility and
default/fallback paths. End-to-end full-scan parity uses synthetic wide schemas, multiple row
groups, compressed V1/V2 pages and small/large page targets.

Window capacity was swept at 512, 1024, 2048 and 4096 slots with 170, 4096 and
16,384-value batches over bit-packed and short-run pages. Time differences were
within about 2% (noise), so the smallest Arrow-equivalent cap of 1024 was
chosen; it only changes allocation for batches above 1024 values.

Eager versus windowed, same run, eight twenty-iteration samples each (benchstat
medians, p=0.000–0.001):

| Workload | Eager time | Windowed time | Eager B/op | Windowed B/op |
| --- | ---: | ---: | ---: | ---: |
| 4096 IDs, one RLE run | 7.77 us | 2.50 us (−68%) | 16,601 | 393 |
| 4096 IDs, bit-packed | 7.82 us | 6.55 us (−16%) | 16,601 | 905 |
| 1,048,576 IDs, one RLE run | 1.83 ms | 0.44 ms (−76%) | 4,194,522 | 393 |
| 1,048,576 IDs, bit-packed | 1.85 ms | 1.46 ms (−21%) | 4,194,524 | 905 |
| Wide file, 4 KiB page target | 21.77 ms | 19.54 ms (−10%) | 12.54 MiB | 4.14 MiB |
| Wide file, 4 MiB page target | 17.87 ms | 16.45 ms (−8%) | 14.35 MiB | 2.27 MiB |

Wide-file allocation counts fall 19.3% (small pages) and 6.5% (large). A local
sweep over 1/8/64/4096-value run lengths, 200- and 256-entry dictionaries and
170/4096-value batches found the windowed path faster than eager in every
case except fully bit-packed pages with a non-power-of-two dictionary, which
are within a few percent of eager (validation's second unpack).

The wide-file benchmark includes 64 dictionary string columns, fallback
columns and 32,769 rows. The microbenchmark includes page construction,
validation and complete small-batch reads; the eager microbenchmark allocates
its decode destination while the end-to-end benchmark exercises normal pooling.
Bytes/op measures allocation volume, not peak live heap or production memory.
These short local runs demonstrate the index allocation bound and tradeoffs,
not deployment qualification or a universal throughput improvement.
