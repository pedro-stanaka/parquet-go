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
inside RLE runs and bit-packed groups. Index storage is at most 4096 int32 slots
(16 KiB) per value reader. A column reader, including the column readers behind
ReadRows, owns and reuses one such window across pages and seeks, releasing it
on Close. Independent Page.Values readers use independent windows. Dictionary
lookup produces values that reference dictionary contents, never mutable index
windows. Existing page reference counting and row byte-value lifetime rules
remain in effect.

Before returning a page, the encoded stream is structurally validated and
logical IDs are checked against the dictionary. RLE runs are validated without
expansion; bit-packed IDs are checked in fixed windows unless the dictionary
covers every ID representable at that width. Every run is still structurally
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
- Bounds scans with bounded scratch space rather than materializing indexes.
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
  Validation and cursor/window overhead still add CPU work. Small-page
  workloads can allocate more than the default pooled eager decoder.

## Synthetic qualification

Commands (Go on darwin/arm64, Apple M3 Pro):

```sh
gofmt -w column.go column_chunk.go config.go dictionary_window*.go
go test ./...
go test -race ./...
go test -run '^$' -bench '^BenchmarkWindowedDictionary(Read|WideFile)$' \
  -benchmem -benchtime=10x -count=3
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

Representative medians from three ten-iteration benchmark runs after the
ownership and block-unpacking fixes (each metric independently medianed):

| Workload | Eager bytes/op | Windowed bytes/op | Eager time | Windowed time | Eager/windowed allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4096 IDs, RLE | 16,755 | 16,915 | 20.921 us | 26.800 us | 3 / 3 |
| 4096 IDs, bit-packed | 16,755 | 16,915 | 17.700 us | 20.521 us | 3 / 3 |
| 1,048,576 IDs, RLE | 4,194,688 | 16,915 | 1.899 ms | 2.429 ms | 3 / 3 |
| 1,048,576 IDs, bit-packed | 4,194,675 | 16,915 | 1.822 ms | 2.361 ms | 3 / 3 |
| Wide file, 4 KiB page target | 13,150,943 | 5,316,732 | 21.005 ms | 23.249 ms | 33,774 / 27,257 |
| Wide file, 4 MiB page target | 15,041,799 | 3,481,696 | 17.604 ms | 19.720 ms | 5,397 / 5,059 |

Packed microbenchmark time is now 1.16x eager for 4096 IDs and 1.30x for
1,048,576 IDs, rather than the earlier scalar implementation's roughly 3x.
These are separate short runs, not a controlled old/new CPU comparison.
Wide-file allocation volume falls 59.6% (small page target) and 76.9% (large),
while elapsed time rises 10.7% and 12.0% against the paired eager runs.
Small-page allocation counts fall 19.3% after returning encoded buffers normally.
No universal CPU improvement over eager decoding is claimed.

The wide-file benchmark includes 64 dictionary string columns, fallback
columns and 32,769 rows. The microbenchmark includes page construction,
validation and complete small-batch reads; the eager microbenchmark allocates
its decode destination while the end-to-end benchmark exercises normal pooling.
Bytes/op measures allocation volume, not peak live heap or production memory.
These short local runs demonstrate the index allocation bound and tradeoffs,
not deployment qualification or a universal throughput improvement.
