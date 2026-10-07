/// A compressed RNTuple page, which no other fixture and neither corpus has.
///
/// Every other RNTuple case here calls `opts.SetCompression(0)`, which keeps the
/// envelopes readable in place so the assertions are about the format rather
/// than a codec. With compression off, though, an `RBlob` payload is raw and the
/// container's compression invariants never run on one (unnoticed until
/// 2026-09-22). With compression on they run, and they fail, because a sealed
/// page is `blocks || 8-byte XXH3 checksum` and the checksum lies outside
/// `fObjLen` and past the end of the block chain
/// (`root/tree/ntuple/src/RPageStorage.cxx:751`,
/// `root/tree/ntuple/inc/ROOT/RPageStorage.hxx:74`).
///
/// One column, so the file holds one page and the arithmetic is readable by
/// hand. The values run 0,0,...,1,1,... in runs of 64 so that zlib has something
/// to find; a page of distinct values would be stored raw.
/// zlib rather than RNTuple's default zstd, because `Compression.md` already
/// pins the `ZL` block header and this case is about the page framing around it.
void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();
   auto fn = model->MakeField<std::int32_t>("n");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(101);   // ZLIB, level 1

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "ntpl", out, opts);
   for (int i = 0; i < 512; ++i) {
      *fn = i / 64;
      writer->Fill();
   }
}
