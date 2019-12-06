// Copyright 2019, Trail of Bits, Inc. All rights reserved.


#include <iostream>
#include <drlojekyll/Display/DisplayManager.h>


#pragma once

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
    class OutputStream {
    public:
        ~OutputStream(void);

        OutputStream(DisplayManager &display_manager_, std::ostream &os_);

        OutputStream &operator<<(Token tok);

        OutputStream &operator<<(DisplayRange range);

        OutputStream &operator<<(ParsedDeclaration decl);

        OutputStream &operator<<(ParsedPredicate pred);

        OutputStream &operator<<(ParsedAggregate aggregate);

        OutputStream &operator<<(ParsedClause clause);

        OutputStream &operator<<(ParsedModule module);

        template <typename T>
        OutputStream &operator<<(T val);

    private:
        DisplayManager display_manager;
        std::ostream &os;
    };

}  // namespace hyde