// Copyright 2021, Trail of Bits. All rights reserved.

#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include "Util.h"

namespace hyde {
namespace flat {
namespace {

static void DeclareType(ParsedModule module, TypeLoc type,
                        OutputStream &os) {
  switch (type.UnderlyingKind()) {
    case TypeKind::kInvalid:
      assert(false);
      os << "???"; break;
    case TypeKind::kBoolean: os << "bool"; break;
    case TypeKind::kSigned8: os << "int8"; break;
    case TypeKind::kSigned16: os << "int16"; break;
    case TypeKind::kSigned32: os << "int32"; break;
    case TypeKind::kSigned64: os << "int64"; break;
    case TypeKind::kUnsigned8: os << "uint8"; break;
    case TypeKind::kUnsigned16: os << "uint16"; break;
    case TypeKind::kUnsigned32: os << "uint32"; break;
    case TypeKind::kUnsigned64: os << "uint64"; break;
    case TypeKind::kFloat: os << "float32"; break;
    case TypeKind::kDouble: os << "float64"; break;
    case TypeKind::kBytes: os << "[ubyte]"; break;
    case TypeKind::kForeignType:
      if (auto ft = module.ForeignType(type); ft) {
        if (ft->IsEnum()) {
          os << ft->Name();
        } else {
          if (auto code = ft->CodeToInline(Language::kFlatBuffer); code) {
            os << (*code);
          } else {
            os << ft->Name();
          }
        }
      } else {
        assert(false);
        os << "???";
      }
      break;
  }
}

//static void DeclareTable(ParsedModule module, DataTable table,
//                         OutputStream &os) {
//
//  auto cols = table.Columns();
//  os << os.Indent() << "table Table" << table.Id() << " {\n";
//  os.PushIndent();
//
//  for (DataColumn col : cols) {
//    os << os.Indent() << "col_" << col.Id() << ":";
//    DeclareType(module, col.Type(), os);
//    os << ";\n";
//  }
//
//  os.PopIndent();
//  os << os.Indent() << "}\n\n";
//}
//
//static void DeclareTables(Program program, ParsedModule module,
//                          OutputStream &os) {
//  for (DataTable table : program.Tables()) {
//    DeclareTable(module, table, os);
//  }
//}

//static void DeclarePredicate(ParsedModule module, ParsedDeclaration decl,
//                             OutputStream &os) {
//  os << os.Indent() << "table " << decl.Name() << "_" << decl.Arity()
//     << " {\n";
//  os.PushIndent();
//  for (ParsedParameter param : decl.Parameters()) {
//    os << os.Indent() << param.Name() << ":";
//    DeclareType(module, param.Type(), os);
//    os << ";\n";
//  }
//  os.PopIndent();
//  os << os.Indent() << "}\n\n";
//}

//using TableIOList = std::vector<std::pair<DataTable, QueryIO>>;
//
//// Find the tables associated with messages.
//static TableIOList FindMessageTables(Program program) {
//  TableIOList ret;
//
//  for (DataTable table : program.Tables()) {
//    for (QueryView view : table.Views()) {
//      if (view.IsSelect()) {
//        QuerySelect select = QuerySelect::From(view);
//        if (select.IsStream()) {
//          QueryStream stream = QueryStream::From(select);
//          if (stream.IsIO()) {
//            ret.emplace_back(table, QueryIO::From(stream));
//          }
//        }
//      } else if (view.IsInsert()) {
//        QueryInsert insert = QueryInsert::From(view);
//        if (insert.IsStream()) {
//          QueryStream stream = QueryStream::From(insert);
//          if (stream.IsIO()) {
//            ret.emplace_back(table, QueryIO::From(stream));
//          }
//        }
//      }
//    }
//  }
//  return ret;
//}

static void DeclareMessages(ParsedModule module,
                            const std::vector<ParsedMessage> &messages,
                            OutputStream &os) {

  bool any_outputs = false;
  bool any_differential_outputs = false;
  bool any_inputs = false;
  bool any_differential_inputs = false;

  for (ParsedEnumType type : module.EnumTypes()) {
    os << os.Indent() << "enum " << type.Name();
    if (auto ut = type.UnderlyingType(); ut.IsValid()) {
      os << " : ";
      DeclareType(module, ut, os);
    }
    os << " {";
    os.PushIndent();

    auto sep = "\n";
    for (ParsedForeignConstant enumerator : type.Enumerators()) {
      os << sep << os.Indent() << enumerator.Name();
      if (auto val = enumerator.Constructor(); !val.empty()) {
        os << " = " << val;
      }
      sep = ",\n";
    }
    os << "\n";
    os.PopIndent();
    os << "}\n\n";
  }

  // Declare each message as a structure.
  for (ParsedMessage message : messages) {
    if (message.IsReceived()) {
      any_inputs = true;
    } else if (message.IsPublished()) {
      any_outputs = true;
    }

    ParsedDeclaration decl(message);

    os << os.Indent() << "table ";
    if (message.IsReceived()) {
      os << "Message_";
    } else {
      os << "Message_";
    }
    os << message.Name() << "_"
       << message.Arity() << " {\n";
    os.PushIndent();

    for (ParsedParameter param : decl.Parameters()) {
      os << os.Indent() << param.Name() << ":";
      DeclareType(module, param.Type(), os);
      os << ";\n";
    }

    os.PopIndent();
    os << os.Indent() << "}\n\n";
  }

  if (any_inputs) {
    // Create a table representing lists of input messages that can be
    // received and added.
    os << os.Indent() << "table AddedInputMessage {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsReceived()) {
        os << os.Indent() << message.Name() << "_" << message.Arity()
           << ":[Message_" << message.Name() << "_" << message.Arity()
           << "];\n";

        if (message.IsDifferential()) {
          any_differential_inputs = true;
        }
      }
    }
    os.PopIndent();
    os << "}\n\n";

    // Create a table representing lists of input messages that can be
    // received and removed.
    if (any_differential_inputs) {
      os << os.Indent() << "table RemovedInputMessage {\n";
      os.PushIndent();
      for (ParsedMessage message : messages) {
        if (message.IsReceived() && message.IsDifferential()) {
          os << os.Indent() << message.Name() << "_" << message.Arity()
             << ":[Message_" << message.Name() << "_" << message.Arity()
             << "];\n";
        }
      }
      os.PopIndent();
      os << "}\n\n";
    }
  }

  os << os.Indent() << "table InputMessage {\n";
  if (any_inputs) {
    os.PushIndent();
    os << os.Indent() << "added:AddedInputMessage;\n";
    if (any_differential_inputs) {
      os << os.Indent() << "removed:RemovedInputMessage;\n";
    }
    os.PopIndent();
  }
  os << os.Indent() << "}\n\n";

  // Create a table representing lists of output messages that can be
  // sent and were added.
  os << os.Indent() << "table AddedOutputMessage {\n";
  os.PushIndent();

  for (ParsedMessage message : messages) {
    if (message.IsPublished()) {
      os << os.Indent() << message.Name() << "_" << message.Arity()
         << ":[Message_" << message.Name() << "_" << message.Arity()
         << "];\n";

      if (message.IsDifferential()) {
        any_differential_outputs = true;
      }
    }
  }
  os.PopIndent();
  os << "}\n\n";

  // Create a table representing lists of input messages that can be
  // sent and were removed.
  if (any_differential_outputs) {
    os << os.Indent() << "table RemovedOutputMessage {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsPublished() && message.IsDifferential()) {
        os << os.Indent() << message.Name() << "_" << message.Arity()
           << ":[Message_" << message.Name() << "_" << message.Arity()
           << "];\n";
      }
    }
    os.PopIndent();
    os << "}\n\n";
  }

  // Create a table representing lists of output messages that can be
  // published.
  os << os.Indent() << "table OutputMessage {\n";
  if (any_outputs) {
    os.PushIndent();
    os << os.Indent() << "added:AddedOutputMessage;\n";
    if (any_differential_outputs) {
      os << os.Indent() << "removed:RemovedOutputMessage;\n";
    }
    os.PopIndent();
  }
  os << os.Indent() << "}\n\n";
}

static void DeclareQueries(ParsedModule module,
                           const std::vector<ParsedQuery> &queries,
                           OutputStream &os) {

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    if (!decl.IsFirstDeclaration()) {
      continue;
    }

    // Declare a table that has one field for each parameter of the
    // query, and then if there are free parameters, then a vector of structs
    // for the free parameters. This represents the response data structure.
    os << os.Indent() << "table " << query.Name() << "_"
       << query.Arity() << " {\n";
    os.PushIndent();

    for (ParsedParameter param : decl.Parameters()) {
      os << os.Indent() << param.Name() << ":";
      DeclareType(module, param.Type(), os);
      os << ";\n";
    }

    os.PopIndent();
    os << os.Indent() << "}\n\n";
  }

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);

    // Declare a table that has one field for each bound parameter of the
    // query. This represents the request data structure.
    os << os.Indent() << "table " << query.Name() << "_"
       << decl.BindingPattern() << " {\n";
    os.PushIndent();

    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << os.Indent() << param.Name() << ":";
        DeclareType(module, param.Type(), os);
        os << ";\n";
      }
    }

    os.PopIndent();
    os << os.Indent() << "}\n\n";
  }
}

static void DeclareService(Program program, ParsedModule module,
                           const std::vector<ParsedQuery> &queries,
                           OutputStream &os) {

  os << os.Indent() << "table Client {\n";
  os.PushIndent();

  os << os.Indent() << "name:string;\n";

  // Declare a service.
  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "table Empty {}\n\n"
     << os.Indent() << "rpc_service Datalog {\n";
  os.PushIndent();

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    os << os.Indent() << "Query_" << query.Name() << "_"
       << decl.BindingPattern() << "(" << query.Name() << "_"
       << decl.BindingPattern() << "):" << query.Name()
       << "_" << decl.Arity();

    auto all_bound = true;
    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() != ParameterBinding::kBound) {
        all_bound = false;
        break;
      }
    }

    if (!all_bound) {
      os << " (streaming: \"server\")";
    }

    os << ";\n";
  }

  // Apply an input message to the database, producing an output message.
  os << os.Indent() << "Publish(InputMessage):Empty;\n"
     << os.Indent() << "Subscribe(Client):OutputMessage (streaming: \"server\");\n";

  os.PopIndent();
  os << os.Indent() << "}\n\n";
}

}  // namespace

// Emits a FlatBuffer schema file.
void GenerateInterfaceCode(const Program &program, OutputStream &os) {
  os << "// Auto-generated file\n\n";

  const auto module = program.ParsedModule();
  const auto db_name = module.DatabaseName();

  if (db_name) {
    os << "namespace " << db_name->NameAsString() << ";\n\n";
  }

  auto queries = Queries(module);
  auto messages = Messages(module);
  auto inlines = Inlines(module, Language::kFlatBuffer);

  // Ideally, type names.
  for (auto code : inlines) {
    if (code.IsPrologue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

//  DeclareTables(program, module, os);

//  auto table_ios = FindMessageTables(program);

  DeclareMessages(module, messages, os);
  DeclareQueries(module, queries, os);
  DeclareService(program, module, queries, os);

  // Other things??
  for (auto code : inlines) {
    if (code.IsEpilogue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << os.Indent() << "root_type InputMessage;\n\n";
}

}  // namespace flat
}  // namespace hyde
