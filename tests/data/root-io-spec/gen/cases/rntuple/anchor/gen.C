/// The first RNTuple fixture: an anchor, a header envelope and a footer
/// envelope written by the pinned ROOT.
///
/// spec/05-rntuple/ tracks ROOT's own specification rather than restating it,
/// so this case serves the audit rather than the format: it pins the bytes that
/// ERRATA 1, 2, 3 and 5 are about, on a file written by 6.40.04. Before it,
/// every byte in those errata came from RNTuple.root in the CERN corpus: one
/// file, written by ROOT 6.35/01, which is older than the pinned release and
/// has an older format version (minor 0 against minor 2).
///
/// Compression is off so the envelopes are readable in place; RNTuple defaults
/// to zstd, which would make every assertion below an assertion about zstd.
///
/// Two scalar fields and three entries is the smallest input that still has a
/// page per column, a cluster, and a page list.
void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();
   auto fx = model->MakeField<float>("x");
   auto fn = model->MakeField<std::int32_t>("n");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "ntpl", out, opts);
   for (int i = 0; i < 3; ++i) {
      *fx = 0.5f * i;
      *fn = i;
      writer->Fill();
   }
}
