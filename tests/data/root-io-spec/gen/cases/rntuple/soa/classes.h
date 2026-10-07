/// A record type and the SoA layout of it, for the RNTuple SoA field.
/// spec/05-rntuple/BinaryFormatSpecification.md, "Classes representing a SoA
/// layout of an underlying record type".
///
/// The SoA class mirrors the record's members as RVecs, and is marked in the
/// dictionary with the `rntuple.SoARecord` attribute -- which gen.C sets at
/// runtime, since ACLiC cannot carry a selection file.
///
/// Both class versions must be equal, which the specification does not say.
/// ROOT refuses otherwise: "version mismatch between SoA type and underlying
/// record type: 7 vs. 3" (root/tree/ntuple/src/RFieldMeta.cxx:707). They are
/// both 3 here, so the version in the field record does not identify the class
/// it came from; the checksum does.
#ifndef RNTUPLE_SOA_H
#define RNTUPLE_SOA_H

#include <ROOT/RVec.hxx>
#include <Rtypes.h>

struct RNPointRecord {
   float fPx = 0.0f;
   float fPy = 0.0f;
   ClassDefNV(RNPointRecord, 3);
};

struct RNPointSoA {
   ROOT::RVec<float> fPx;
   ROOT::RVec<float> fPy;
   ClassDefNV(RNPointSoA, 3);
};

#endif
