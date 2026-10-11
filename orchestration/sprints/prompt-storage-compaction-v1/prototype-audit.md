# Supplied prototype audit

The supplied local prototype was inspected statically and was not executed.
Its SHA-256 was:

```text
bdff8cac4a9acf581a236a032117c60309e436d5b92474e6eccc5fe49c3dabe7
```

The exact bytes are preserved at
`reference/prompt_storage_compactor_prototype.py` with Git text conversion
disabled for that file.  It is reference-only and must not be executed against
live paths.

## Useful evidence

The prototype:

- measures selected JSON field weights;
- finds the observed timeline/activity and patches/code-patches duplicates;
- extracts nine known heavy values into gzip files keyed by SHA-256 of a
  Python-produced canonical JSON encoding;
- checks blob hashes during read;
- reconstructs Python values and compares deep equality plus canonical digest;
- demonstrates why a light manifest plus immutable CAS is a promising
  direction.

## Gaps that production work must close

1. It does not reconstruct the original raw file bytes. Parsing and canonical
   reserialization loses whitespace, key order, numeric spelling, and duplicate
   object members.
2. It blobs whole cumulative arrays/text. Appending one record therefore creates
   another full blob and can preserve quadratic growth across transitions.
3. Its configured `min_blob_size` is unused; extraction uses a hard-coded
   threshold.
4. Publication has no writer lock/exclusive create, unique temporary file,
   fsync, atomic manifest protocol, reachability transaction, or crash recovery.
   Existing blobs are reused without revalidation.
5. A singleton `$cas_ref` object is unversioned and can collide with user data.
6. Declared sizes, compression, path containment, reference limits, and
   decompression limits are not validated.
7. The safety check covers only four hard-coded paths, hashes only their first
   MiB, does not cover transition archives or all `.prompt` files, permits an
   output inside a live root, and does not fail the process on a violation.
8. Reported savings omit compact manifests, metadata, index data, and filesystem
   overhead.
9. Post-hoc compaction alone does not remove hot-path latency because the current
   server can construct full project state before writing the compact agent
   response.

The prototype is an analysis oracle and fixture idea only.  Agents must not copy
it into the runtime unchanged or run it against live data.
