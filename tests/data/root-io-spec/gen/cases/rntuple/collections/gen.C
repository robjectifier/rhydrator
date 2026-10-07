/// One field per stdlib type the tracked specification maps, to check the
/// fields and columns each one produces.
///
/// The claims being checked are prose rather than a table -- "stored as two
/// fields", "the name of the child field is `_0`", "a repetitive plain field
/// with an attached `Bit` column" -- one paragraph per type in *Stdlib Types and
/// Collections*. Compression is off, so an index column that the document writes
/// as `(Split)Index[64|32]` must come out unsplit here, the same rule the
/// fundamental-types case checks.
///
/// Field order is the document's subsection order, so the decoded schema reads
/// straight against it.
///
/// `std::map` is not here: it has a case of its own, rntuple/map. The map this
/// case once tried, std::map<int, float>, has no compiled dictionary, and ROOT
/// 6.40.04 aborts writing such a map from the interpreter, with `R__ASSERT(0)`
/// in TGenCollectionProxy__VectorNext
/// (root/io/io/src/TGenCollectionProxy.cxx:1528-1530). See
/// spec/05-rntuple/NOTES.md 5.
#include <array>
#include <atomic>
#include <bitset>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <ROOT/RVec.hxx>

void gen(const char *out)
{
   auto model = ROOT::RNTupleModel::Create();

   auto fString   = model->MakeField<std::string>("fString");
   auto fVector   = model->MakeField<std::vector<float>>("fVector");
   auto fRVec     = model->MakeField<ROOT::RVec<float>>("fRVec");
   auto fArray    = model->MakeField<std::array<int, 3>>("fArray");
   auto fVariant  = model->MakeField<std::variant<int, float>>("fVariant");
   auto fPair     = model->MakeField<std::pair<int, float>>("fPair");
   auto fTuple    = model->MakeField<std::tuple<int, float, char>>("fTuple");
   auto fBitset   = model->MakeField<std::bitset<8>>("fBitset");
   auto fUnique   = model->MakeField<std::unique_ptr<float>>("fUnique");
   auto fOptional = model->MakeField<std::optional<float>>("fOptional");
   auto fSet      = model->MakeField<std::set<int>>("fSet");
   auto fAtomic   = model->MakeField<std::atomic<int>>("fAtomic");
   auto fNested   = model->MakeField<std::vector<std::vector<int>>>("fNested");

   // Double32_t has to be requested by name. It is a typedef for double, so
   // MakeField<Double32_t> instantiates the double field and the alias is lost;
   // only the type name keeps it. This field is also the fixture's instance of
   // the exception in ERRATA 7: it keeps a SplitReal32 column in an
   // uncompressed ntuple, where every fundamental type drops to unsplit.
   model->AddField(ROOT::RFieldBase::Create("fDouble32", "Double32_t").Unwrap());

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);

   auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "coll", out, opts);

   *fString = "ab";
   *fVector = {1.0f, 2.0f};
   *fRVec = {3.0f};
   *fArray = {7, 8, 9};
   *fVariant = 5.5f;
   *fPair = {1, 2.0f};
   *fTuple = std::make_tuple(3, 4.0f, 'x');
   *fBitset = std::bitset<8>(0b1010'0001);
   *fUnique = std::make_unique<float>(6.5f);
   *fOptional = 7.5f;
   *fSet = {1, 2, 3};
   fAtomic->store(11);
   *fNested = {{1, 2}, {3}};
   writer->Fill();
}
