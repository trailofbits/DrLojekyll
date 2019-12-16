// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Format.h>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

namespace hyde {
namespace {

static const char *kBeginTable = "<TABLE cellpadding=\"0\" cellspacing=\"0\" border=\"1\"><TR>";
static const char *kEndTable = "</TR></TABLE>";

}  // namespace

OutputStream &operator<<(OutputStream &os, Query query) {
  os << "digraph {\n"
     << "node [shape=none margin=0 nojustify=false labeljust=l];\n";

  for (auto relation : query.Relations()) {
    const auto decl = relation.Declaration();
    const auto arity = decl.Arity();
    os << "t" << relation.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>";

    if (relation.IsNegative()) {
      os << "!";
    }

    os << decl.Name() << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }

    os << kEndTable << ">];\n";
  }

  for (auto constant : query.Constants()) {
    os << "t" << constant.UniqueId() << " [ label=<" << kBeginTable
       << "<TD port=\"p0\">" << constant.Literal() << "</TD>" << kEndTable
       << ">];\n";
  }

  for (auto select : query.Selects()) {
    os << "v" << select.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>SELECT</TD>";
    auto i = 0u;
    for (auto col : select.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\"> &nbsp; </TD>";
      ++i;
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.
    const auto table = select.Table();
    i = 0u;
    for (auto col : select.Columns()) {
      os << "v" << select.UniqueId() << ":c" << col.UniqueId() << " -> t"
         << table.UniqueId() << ":p" << i << ";\n";
      ++i;
    }
  }

  for (auto join : query.Joins()) {
    auto out_col = join.PivotColumn();
    os << "v" << join.UniqueId() << " [ label=<" << kBeginTable
       << "<TD port=\"c" << out_col.UniqueId() << "\" rowspan=\"2\">JOIN</TD>";

    for (auto i = 0u; i < join.Arity(); ++i) {
      auto col = join.NthOutputColumn(i);
      os << "<TD port=\"c" << col.UniqueId() << "\"> &nbsp; </TD>";
    }

    os << "</TR><TR>";
    for (auto i = 0u; i < join.Arity(); ++i) {
      os << "<TD port=\"p" << i << "\"> &nbsp; </TD>";
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.
    for (auto i = 0u; i < join.Arity(); ++i) {
      auto col = join.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << join.UniqueId() << ":p" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto insert : query.Inserts()) {
    os << "i" << insert.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>INSERT</TD>";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      os << "<TD port=\"c" << i << "\"> &nbsp; </TD>";
    }

    os << kEndTable << ">];\n";

    auto table = insert.Relation();
    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      const auto col = insert.NthColumn(i);
      const auto view = QueryView::Containing(col);
      os << "t" << table.UniqueId() << ":p" << i << " -> "
         << "i" << insert.UniqueId() << ":c" << i << ";\n";

      os << "i" << insert.UniqueId() << ":c" << i << " -> "
         << "v" << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  os << "}\n";
  return os;
}

}  // namespace hyde
