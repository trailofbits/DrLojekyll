// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/BAM.h>

#include <bitset>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Sema/BottomUpAnalysis.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {
namespace {

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(
    const DisplayManager &display_manager, const ParsedModule &module,
    std::ostream &cxx_os) {

  BottomUpAnalysis analysis;
  for (auto state : analysis.GenerateStates(module)) {
    FastBindingSIPSScorer scorer;
    if (state->assumption) {
      SIPSGenerator generator(*(state->assumption));
      scorer.VisitBestScoringPermuation(scorer, bam_builder, generator);
    } else {
      SIPSGenerator generator(state->clause);
      scorer.VisitBestScoringPermuation(scorer, bam_builder, generator);
    }
  }
}

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(
    const DisplayManager &display_manager, const Query &query,
    std::ostream &cxx_os) {

}

}  // namespace hyde
