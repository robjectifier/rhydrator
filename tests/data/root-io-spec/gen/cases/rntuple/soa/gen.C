/// A collection stored as a struct of arrays: the SoA flag, 0x08.
///
/// The document says such a field is a collection parent whose principal column
/// is an index, with a child field of the underlying record type named `_0`, that
/// "the field's type name is the type of the SoA class", that "the type checksum
/// and the type version of the SoA class is stored as field information", and that
/// "the SoA flag of the field descriptor must be set" -- the one flag bit no other
/// fixture reaches.
///
/// The layout is chosen by a dictionary attribute, `rntuple.SoARecord`, naming the
/// record type (root/tree/ntuple/src/RFieldUtils.cxx:717-731). ROOT's own field is
/// ROOT::Experimental::RSoAField and prints an experimental warning on
/// construction, which is expected here and not an error.
void gen(const char *out)
{
   auto cl = TClass::GetClass("RNPointSoA");
   cl->CreateAttributeMap();
   cl->GetAttributeMap()->AddProperty("rntuple.SoARecord", "RNPointRecord");

   auto model = ROOT::RNTupleModel::Create();
   model->AddField(std::make_unique<ROOT::Experimental::RSoAField>("fPoints", "RNPointSoA"));
   auto fTag = model->MakeField<int>("fTag");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "soa", out, opts);

   auto soa = writer->GetModel().GetDefaultEntry().GetPtr<RNPointSoA>("fPoints");
   soa->fPx = {1.0f, 2.0f};
   soa->fPy = {3.0f, 4.0f};
   *fTag = 5;
   writer->Fill();
}
