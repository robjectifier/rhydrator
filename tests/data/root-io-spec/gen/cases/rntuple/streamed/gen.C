/// A field serialized by the ROOT streamer: structural role 0x04.
///
/// The document says such a field has two columns -- `(Split)Index[32|64]` and
/// `Byte` -- so "the column representation is identical to a collection of
/// `std::byte`", and that the streamer information for every streamed field in
/// the ntuple is carried once, in the header envelope's extra type information
/// list, as a ROOT-streamed `TList` of `TStreamerInfo` under content identifier
/// 0x00.
///
/// The mode is a dictionary attribute rather than a property of the class, and it
/// is settable at runtime, which is how ROOT's own tests reach this path
/// (root/tree/ntuple/test/rfield_streamer.cxx:54-57). It has to be a MEMBER: a
/// top-level field of a streamer-mode class is refused
/// (root/tree/ntuple/src/RFieldMeta.cxx:95), so the streamed form only exists
/// underneath a native field.
///
/// Compression is off, so the index column keeps its unsplit form and the
/// streamed payload is readable in place.
void gen(const char *out)
{
   auto cl = TClass::GetClass("RNStreamedInner");
   cl->CreateAttributeMap();
   cl->GetAttributeMap()->AddProperty("rntuple.streamerMode", "true");

   auto model = ROOT::RNTupleModel::Create();
   auto fEvent = model->MakeField<RNEvent>("fEvent");
   auto fPlain = model->MakeField<float>("fPlain");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "streamed", out, opts);

   fEvent->fEnergy = 2.5f;
   fEvent->fInner.fA = 3;
   fEvent->fInner.fB = 0.5f;
   fEvent->fInner.fC = {7, 8};
   *fPlain = 1.5f;
   writer->Fill();
}
