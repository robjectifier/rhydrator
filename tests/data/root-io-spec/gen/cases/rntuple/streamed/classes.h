/// A class RNTuple must serialize with the ROOT streamer rather than split.
/// spec/05-rntuple/BinaryFormatSpecification.md, "ROOT streamed types".
///
/// Nothing in the classes themselves forces that: the choice is a dictionary
/// attribute, `rntuple.streamerMode`
/// (root/tree/ntuple/src/RFieldUtils.cxx:702-715), which gen.C sets on
/// RNStreamedInner's TClass before building the model, the same way ROOT's own
/// unit test does (root/tree/ntuple/test/rfield_streamer.cxx:54-57).
///
/// The streamed class is a MEMBER rather than the top-level field, because a
/// top-level field of a streamer-mode class is refused outright:
/// "RNStreamed has streamer mode enforced, not supported as native RNTuple class"
/// (root/tree/ntuple/src/RFieldMeta.cxx:95). The streamed form is reachable only
/// underneath a native field.
#ifndef RNTUPLE_STREAMED_H
#define RNTUPLE_STREAMED_H

#include <vector>

struct RNStreamedInner {
   int fA = 0;
   float fB = 0.0f;
   std::vector<int> fC;
};

struct RNEvent {
   float fEnergy = 0.0f;
   RNStreamedInner fInner;
};

#endif
