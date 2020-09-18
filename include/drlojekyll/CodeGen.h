// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class Query;
class ParsedModule;
class OutputStream;

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(const ParsedModule &module, const Query &query,
                  OutputStream &os);

}  // namespace hyde
