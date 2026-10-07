/// Linked attribute sets: two attribute set RNTuples linked from a main RNTuple's
/// footer.
///
/// spec/05-rntuple/BinaryFormatSpecification.md, "Linked Attribute Sets" and
/// "Linked Attribute Set Record Frame". Each attribute set is an RNTuple of its
/// own, with its own header, pages, page list, footer and anchor, and the main
/// footer's last list frame holds one record per set: the attribute schema
/// version, the anchor's length, a locator to the anchor and the set's name.
///
/// ROOT 6.40.04 writes attribute sets only through ROOT::Experimental, and only
/// from a writer created by RNTupleWriter::Append, because the set's anchor is
/// written through the TFile (root/tree/ntuple/inc/ROOT/RNTupleWriter.hxx:273-274,
/// root/tree/ntuple/src/RMiniFile.cxx:1322-1329). So this case opens the TFile
/// itself, uncompressed like every other RNTuple case, and appends to it.
///
/// Two sets, with distinct names and different user schemas:
///
///   runs   run (std::int32_t), weight (float); ranges [0, 3) and [3, 6)
///   flags  flag (std::uint16_t);  a range of length 0 at entry 3, committed
///          first, then [1, 5), which was begun before it and so overlaps it
///
/// The zero-length range is the one the document says "is valid and refers to an
/// empty range". The order of the flags entries on disk is the commit order,
/// not the order of the range starts.
///
/// One thing here is not ROOT's default behaviour, and it is there so that the
/// byte assertions hold on every platform. Committing an attribute set calls
/// TFile::Write (root/tree/ntuple/src/RMiniFile.cxx:1372), which writes the
/// file's StreamerInfo record in the middle of the file, before the main
/// RNTuple's footer. That record holds ROOT::RNTuple's streamer info, which is 28
/// bytes longer with libstdc++ than with libc++ (ElementTypes.md 2.4), so every
/// offset after it, the footer's attribute set list included, would differ
/// between Linux and macOS. The macro therefore writes the empty StreamerInfo
/// list first and marks ROOT::RNTuple's info as used without flagging the index
/// as changed, which TFile::WriteStreamerInfo takes to mean "nothing new"
/// (root/io/io/src/TFile.cxx:3497-3503). Every Write during the commit then
/// skips the record, and the macro raises the flag before closing, so the real
/// record is written once, at the end of the file. Nothing else changes: the
/// RNTuple bytes are what ROOT writes, only one TFile record moves.
#include <cstdint>
#include <memory>

void gen(const char *out)
{
   auto file = std::unique_ptr<TFile>(TFile::Open(out, "RECREATE", "", 0));

   // Hold the StreamerInfo record back until the end. See above.
   file->WriteStreamerInfo();
   const auto rntupleInfo = ROOT::RNTuple::Class()->GetStreamerInfo()->GetNumber();
   file->GetClassIndex()->fArray[rntupleInfo] = 1;

   auto model = ROOT::RNTupleModel::Create();
   auto fX = model->MakeField<float>("x");

   ROOT::RNTupleWriteOptions opts;
   opts.SetCompression(0);
   auto writer = ROOT::RNTupleWriter::Append(std::move(model), "ntpl", *file, opts);

   auto runsModel = ROOT::RNTupleModel::Create();
   auto fRun = runsModel->MakeField<std::int32_t>("run");
   auto fWeight = runsModel->MakeField<float>("weight");
   auto runs = writer->CreateAttributeSet(std::move(runsModel), "runs");

   auto flagsModel = ROOT::RNTupleModel::Create();
   auto fFlag = flagsModel->MakeField<std::uint16_t>("flag");
   auto flags = writer->CreateAttributeSet(std::move(flagsModel), "flags");

   // Entry i holds x = 0.5 * (i + 1).
   auto run1 = runs->BeginRange();              // runs [0, ...
   *fX = 0.5f;
   writer->Fill();                              // entry 0
   auto wide = flags->BeginRange();             // flags [1, ...
   for (int i = 1; i < 3; ++i) {
      *fX = 0.5f * (i + 1);
      writer->Fill();                           // entries 1, 2
   }
   *fRun = 1;
   *fWeight = 0.25f;
   runs->CommitRange(std::move(run1));          // runs [0, 3)

   auto run2 = runs->BeginRange();              // runs [3, ...
   auto empty = flags->BeginRange();            // flags [3, ...
   *fFlag = 7;
   flags->CommitRange(std::move(empty));        // flags [3, 3): length 0
   for (int i = 3; i < 5; ++i) {
      *fX = 0.5f * (i + 1);
      writer->Fill();                           // entries 3, 4
   }
   *fFlag = 9;
   flags->CommitRange(std::move(wide));         // flags [1, 5)
   *fX = 3.0f;
   writer->Fill();                              // entry 5
   *fRun = 2;
   *fWeight = 0.75f;
   runs->CommitRange(std::move(run2));          // runs [3, 6)

   // Destroying the writer commits both attribute sets, then the main RNTuple.
   writer.reset();
   file->GetClassIndex()->fArray[0] = 1;
   file->Close();
}
