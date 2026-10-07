/// Untyped collections and records: fields with a structural role and no type.
///
/// The document's *Untyped collections and records* section says such a field has
/// "a collection or record role and an empty type name", that only a top-level
/// field or a direct subfield of an untyped field may be untyped, and that the
/// on-disk representation is otherwise that of a `std::vector` or of a
/// user-defined class. No C++ type produces one: they are built field by field,
/// which is what this case does.
///
/// Compression is off, so the index column keeps its unsplit form.
#include <memory>
#include <vector>

void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   // An untyped record: an RRecordField built from its members rather than from
   // a class. Its type name is empty; the members are ordinary fields.
   std::vector<std::unique_ptr<ROOT::RFieldBase>> members;
   members.push_back(ROOT::RFieldBase::Create("fX", "float").Unwrap());
   members.push_back(ROOT::RFieldBase::Create("fN", "std::int32_t").Unwrap());
   model->AddField(std::make_unique<ROOT::RRecordField>("fRecord", std::move(members)));

   // An untyped collection, whose item field carries the `_0` name a typed
   // collection's item would have.
   auto item = ROOT::RFieldBase::Create("_0", "float").Unwrap();
   model->AddField(ROOT::RVectorField::CreateUntyped("fColl", std::move(item)));

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "untyped", out, opts);
   writer->Fill();
}
