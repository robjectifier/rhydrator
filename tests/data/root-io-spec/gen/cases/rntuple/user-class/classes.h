/// A user-defined class, a base class and two enums, for the RNTuple type
/// mapping. spec/05-rntuple/BinaryFormatSpecification.md, "User-defined classes"
/// and "User-defined enums".
///
/// Nothing here has a ClassDef: a dictionary is all RNTuple asks for, and ACLiC
/// generates one for every class in this header. A member marked `//!` must not
/// reach the file; a reader that expected it would shift every field that
/// follows.
#ifndef RNTUPLE_USER_CLASS_H
#define RNTUPLE_USER_CLASS_H

#include <cstdint>
#include <string>
#include <vector>

/// Unscoped, with the default underlying type. The enumerators carry an `RN`
/// prefix because ROOT's own `PDG_t` already puts `kUp`, `kDown` and `kStrange`
/// in the global namespace, and an unscoped enum that repeats them does not
/// compile.
enum ERNFlavour { kRNUp = 0, kRNDown = 1, kRNStrange = 2 };

/// Scoped, with an underlying type that is not int, so the child field's type
/// says which one it is.
enum class ERNCharge : std::int16_t { kNeg = -1, kZero = 0, kPos = 1 };

/// A direct base class, which the document says is a subfield named `:_0`.
struct RNBase {
   std::int32_t fBaseId = 0;
};

struct RNHit : RNBase {
   float fEnergy = 0.0f;
   ERNFlavour fFlavour = kRNUp;
   ERNCharge fCharge = ERNCharge::kZero;
   std::vector<float> fSamples;
   std::string fLabel;
   double fScratch = 0.0;   //! transient: must not appear on disk
};

#endif
