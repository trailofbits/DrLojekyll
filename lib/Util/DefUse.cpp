// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util.h>

namespace hyde {

User::~User(void) {}

void User::Update(uint64_t next_timestamp) {
  (void) next_timestamp;
}

uint64_t User::gNextTimestamp{1};

}  // namespace hyde
