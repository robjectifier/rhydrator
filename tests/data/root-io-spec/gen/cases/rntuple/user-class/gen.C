/// A user-defined class in an RNTuple: the record shape, base classes, enums and
/// the transient member that must not be there.
///
/// The claims are in "User-defined classes" and "User-defined enums" of the
/// tracked specification: a record parent with no columns, member subfields named
/// after the C++ members, base classes as `:_0`, `:_1`, and an enum as a plain
/// parent with a single `_0` of the underlying integer type. The type version and
/// the type checksum of the class reach the field record too, which is what lets a
/// reader match a class to a dictionary.
///
/// Compression is off, so an index column keeps its unsplit form, as in the
/// sibling cases.
void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   auto fHit      = model->MakeField<RNHit>("fHit");
   auto fHits     = model->MakeField<std::vector<RNHit>>("fHits");
   auto fFlavour  = model->MakeField<ERNFlavour>("fFlavour");
   auto fCharge   = model->MakeField<ERNCharge>("fCharge");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "hits", out, opts);

   fHit->fBaseId = 7;
   fHit->fEnergy = 1.5f;
   fHit->fFlavour = kRNDown;
   fHit->fCharge = ERNCharge::kNeg;
   fHit->fSamples = {0.25f, 0.5f};
   fHit->fLabel = "hit";
   fHit->fScratch = 99.0;          // transient: nothing must change on disk

   fHits->resize(1);
   fHits->at(0).fBaseId = 8;

   *fFlavour = kRNStrange;
   *fCharge = ERNCharge::kPos;
   writer->Fill();
}
