/// One field per fundamental C++ type, to check the column each one lands in.
///
/// The tracked specification's "Fundamental Types" table marks a default column
/// per type with an asterisk, and then says: "If the ntuple is stored
/// uncompressed, the default changes from split encoding to non-split encoding
/// where applicable." Both halves are checkable, and this case checks the
/// uncompressed half -- thirteen types, thirteen columns, compression off.
///
/// The field order is the table's column order, so the decoded schema can be
/// read straight against it.
void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   auto fBool   = model->MakeField<bool>("fBool");
   auto fByte   = model->MakeField<std::byte>("fByte");
   auto fChar   = model->MakeField<char>("fChar");
   auto fI8     = model->MakeField<std::int8_t>("fI8");
   auto fU8     = model->MakeField<std::uint8_t>("fU8");
   auto fI16    = model->MakeField<std::int16_t>("fI16");
   auto fU16    = model->MakeField<std::uint16_t>("fU16");
   auto fI32    = model->MakeField<std::int32_t>("fI32");
   auto fU32    = model->MakeField<std::uint32_t>("fU32");
   auto fI64    = model->MakeField<std::int64_t>("fI64");
   auto fU64    = model->MakeField<std::uint64_t>("fU64");
   auto fFloat  = model->MakeField<float>("fFloat");
   auto fDouble = model->MakeField<double>("fDouble");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "types", out, opts);

   *fBool = true;
   *fByte = std::byte{0x2a};
   *fChar = 'z';
   *fI8 = -8;   *fU8 = 8;
   *fI16 = -16; *fU16 = 16;
   *fI32 = -32; *fU32 = 32;
   *fI64 = -64; *fU64 = 64;
   *fFloat = 0.5f;
   *fDouble = 0.25;
   writer->Fill();
}
