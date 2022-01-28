// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/Parse/ModuleIterator.h>

namespace hyde {

std::vector<ParsedFunctor> Functors(ParsedModule module) {
  std::vector<ParsedFunctor> decls;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedFunctor query : sub_module.Functors()) {
      ParsedDeclaration decl(query);
      if (decl.IsFirstDeclaration()) {
        for (ParsedDeclaration redecl : decl.UniqueRedeclarations()) {
          decls.push_back(ParsedFunctor::From(redecl));
        }
      }
    }
  }
  return decls;
}

std::vector<ParsedQuery> Queries(ParsedModule module) {
  std::vector<ParsedQuery> decls;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedQuery query : sub_module.Queries()) {
      ParsedDeclaration decl(query);
      if (decl.IsFirstDeclaration()) {
        for (ParsedDeclaration redecl : decl.UniqueRedeclarations()) {
          decls.push_back(ParsedQuery::From(redecl));
        }
      }
    }
  }
  return decls;
}

std::vector<ParsedMessage> Messages(ParsedModule module) {
  std::vector<ParsedMessage> decls;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedMessage query : sub_module.Messages()) {
      ParsedDeclaration decl(query);
      if (decl.IsFirstDeclaration()) {
        for (ParsedDeclaration redecl : decl.UniqueRedeclarations()) {
          decls.push_back(ParsedMessage::From(redecl));
        }
      }
    }
  }
  return decls;
}

std::vector<ParsedInline> Inlines(ParsedModule module, Language lang) {
  std::vector<ParsedInline> inlines;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedInline code : sub_module.Inlines()) {
      if (auto inline_lang = code.Language();
          inline_lang == lang || inline_lang == Language::kUnknown) {
        inlines.push_back(code);
      }
    }
  }
  return inlines;
}

}  // namespace hyde
