/// Projected fields, alias columns and RNTupleCardinality: the parts of the
/// schema that no C++ member produces.
///
/// A projected field presents data that is already in the file under another
/// name or another type. It carries flag 0x02 and a source field ID, and its
/// columns are **alias columns** -- a separate list in the header envelope with
/// no column IDs of their own, which the "Alias columns" section says must never
/// be referenced from the footer or the page list.
///
/// `ROOT::RNTupleCardinality<SizeT>` exists only as a projection: it reads a
/// collection's index column and presents the differences as lengths. Both widths
/// the document allows are here.
///
/// Compression is off, so the physical index column keeps its unsplit form.
#include <string>
#include <vector>

void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   auto fVec = model->MakeField<std::vector<float>>("fVec");
   auto fE   = model->MakeField<float>("fE");

   // A projection of a plain field: same type, another name.
   model->AddProjectedField(ROOT::RFieldBase::Create("fEnergy", "float").Unwrap(),
                            [](const std::string &) { return "fE"; });

   // A projection of the whole collection, which needs a source for the parent
   // and one for the item.
   model->AddProjectedField(
      ROOT::RFieldBase::Create("fAlias", "std::vector<float>").Unwrap(),
      [](const std::string &name) {
         return name == "fAlias" ? "fVec" : "fVec._0";
      });

   // The cardinality of that collection, in both permitted widths.
   model->AddProjectedField(
      ROOT::RFieldBase::Create("fN32", "ROOT::RNTupleCardinality<std::uint32_t>").Unwrap(),
      [](const std::string &) { return "fVec"; });
   model->AddProjectedField(
      ROOT::RFieldBase::Create("fN64", "ROOT::RNTupleCardinality<std::uint64_t>").Unwrap(),
      [](const std::string &) { return "fVec"; });

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "proj", out, opts);

   // Three entries with different collection sizes, so the cardinality values
   // the document works out -- [1, 0, 2] from an index column of [1, 1, 3] --
   // are the ones in the file.
   *fVec = {1.0f};
   *fE = 0.5f;
   writer->Fill();
   fVec->clear();
   *fE = 1.5f;
   writer->Fill();
   *fVec = {2.0f, 3.0f};
   *fE = 2.5f;
   writer->Fill();
}
