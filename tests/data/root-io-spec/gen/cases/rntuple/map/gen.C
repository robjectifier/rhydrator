/// One field per associative container that *Stdlib Types and Collections* maps
/// in its `std::map` subsection: map, unordered_map, multimap and
/// unordered_multimap.
///
/// Each instantiation is one ROOT ships a compiled dictionary for
/// (root/core/clingutils/src/mapLinkdef.h, unordered_mapLinkdef.h,
/// multimapLinkdef.h, unordered_multimapLinkdef.h). This is required: from the
/// interpreter, ROOT 6.40.04 writes a map field only when the class under
/// RNTuple's normalised type name has one, and aborts in Fill() with
/// `R__ASSERT(0)` in TGenCollectionProxy__VectorNext otherwise
/// (root/io/io/src/TGenCollectionProxy.cxx:1528-1530). spec/05-rntuple/NOTES.md 5.
///
/// Compression is off, as in rntuple/collections, so the index columns come out
/// unsplit.
#include <map>
#include <string>
#include <unordered_map>

void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   auto fMap       = model->MakeField<std::map<std::string, int>>("fMap");
   auto fUnordered = model->MakeField<std::unordered_map<int, int>>("fUnordered");
   auto fMulti     = model->MakeField<std::multimap<std::string, int>>("fMulti");
   auto fUnMulti   = model->MakeField<std::unordered_multimap<std::string, int>>("fUnMulti");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "maps", out, opts);

   *fMap = {{"a", 1}, {"bc", 2}};
   *fUnordered = {{7, 70}};
   *fMulti = {{"k", 3}, {"k", 4}};
   *fUnMulti = {{"u", 5}};
   writer->Fill();

   fMap->clear();
   fUnordered->clear();
   fMulti->clear();
   fUnMulti->clear();
   writer->Fill();
}
