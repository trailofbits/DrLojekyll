// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "Util.h"

namespace hyde {

// Emits Python code for the given program to `os`.
void GeneratePythonInterfaceCode(const Program &program, OutputStream &os) {
  os << "# Auto-generated file\n\n"
     << "from __future__ import annotations\n"
     << "from dataclasses import dataclass\n"
     << "from typing import Final, List, Optional, Tuple\n"
     << "try:\n";
  os.PushIndent();
  os << os.Indent() << "from typing import Protocol\n";
  os.PopIndent();
  os << "except ImportError:\n";
  os.PushIndent();
  os << os.Indent() << "from typing_extensions import Protocol #type: ignore\n\n\n";
  os.PopIndent();

  const auto module = program.ParsedModule();

  // Output prologue code.
  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kPython:
          if (code.IsPrologue()) {
            os << code.CodeToInline() << "\n\n\n";
          }
          break;
        default:
          break;
      }
    }
  }

  const auto messages = Messages(module);

  // Input messages to datalog.
  os << os.Indent() << "@dataclass\n"
     << os.Indent() << "class " << gClassName << "InputMessage:\n";

  os.PushIndent();

  for (auto message : messages) {
    if (message.IsPublished()) {
      continue;
    }

    os << os.Indent() << message.Name() << '_' << message.Arity()
       << ": Optional[List[";
    if (1u < message.Arity()) {
      os << "Tuple[";
    }

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    if (1u < message.Arity()) {
      os << ']';
    }
    os << "]]\n";
  }
  os << '\n';

  // Emit a method that will apply everything in this data class to an
  // instance of the database. Returns the number of processed messages.
  os << os.Indent() << "def apply(self, db: 'Database') -> int:\n";
  os.PushIndent();
  os << os.Indent() << "num_messages: int = 0\n";
  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }
    os << os.Indent() << "if self." << message.Name() << '_'
       << message.Arity() << " is not None:\n";
    os.PushIndent();
    os << os.Indent() << "num_messages += len(self." << message.Name() << '_'
       << message.Arity() << ")\n"
       << os.Indent() << "db." << message.Name() << '_' << message.Arity()
       << "(self." << message.Name() << '_' << message.Arity() << ")\n";
    os.PopIndent();
  }
  os << os.Indent() << "return num_messages\n";
  os.PopIndent();
  os.PopIndent();
  os << "\n\n";

  // Implements a class that can build input messages.
  os << os.Indent() << "class " << gClassName << "InputMessageAggregator:\n";
  os.PushIndent();
  os << os.Indent() << "def __init__(self):\n";
  os.PushIndent();
  os << os.Indent() << "self._msg: " << gClassName << "InputMessage = "
     << gClassName << "InputMessage()\n"
     << os.Indent() << "self._num_msgs: int = 0\n\n";
  os.PopIndent();
  for (auto message : messages) {
    if (message.IsPublished()) {
      continue;
    }

    os << os.Indent() << "def " << message.Name() << '_' << message.Arity()
       << "(self";

    for (auto param : message.Parameters()) {
      os << ", " << param.Name() << ": " << TypeName(module, param.Type());
    }
    os << "):\n";
    os.PushIndent();
    os << os.Indent() << "self._num_msgs += 1\n"
       << os.Indent() << "if self._msg." << message.Name() << '_'
       << message.Arity() << " is None:\n";
    os.PushIndent();
    os << os.Indent() << "self._msg." << message.Name() << '_'
       << message.Arity() << " = []\n";
    os.PopIndent();
    os << os.Indent() << "self._msg." << message.Name() << '_'
       << message.Arity() << ".append(";
    if (1u < message.Arity()) {
      os << '(';
    }

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << param.Name();
      sep = ", ";
    }

    if (1u < message.Arity()) {
      os << ')';
    }
    os << ")\n\n";
    os.PopIndent();
  }

  // Emit a method that returns `None` if no messages were published by Datalog,
  // or emits an aggregated message representing all published messages since
  // the last time we asked.
  os << os.Indent() << "def aggregate(self) -> Optional[" << gClassName
     << "InputMessage]:\n";
  os.PushIndent();
  os << os.Indent() << "if not self._num_msgs:\n";
  os.PushIndent();
  os << os.Indent() << "return None\n";
  os.PopIndent();
  os << os.Indent() << "self._num_msgs = 0\n"
     << os.Indent() << "msg = self._msg\n"
     << os.Indent() << "self._msg = " << gClassName << "InputMessage()\n"
     << os.Indent() << "return msg\n\n";
  os.PopIndent();

  os << os.Indent() << "def __len__(self) -> int:\n";
  os.PushIndent();
  os << os.Indent() << "return self._num_msgs\n\n\n";
  os.PopIndent();
  os.PopIndent();

  // Output messages from datalog.
  os << os.Indent() << "@dataclass\n"
     << os.Indent() << "class " << gClassName << "OutputMessage:\n";

  os.PushIndent();

  auto empty = true;

  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    empty = false;

    // Messages published by Dr. Lojekyll have the extra `added` component.
    os << os.Indent() << message.Name() << '_' << message.Arity()
       << ": Optional[List[Tuple[";

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    os << sep << "bool]]]\n";
  }

  if (empty) {
    os << os.Indent() << "pass\n";
  }

  os.PopIndent();
  os << "\n\n";

  // Creates a protocol that describes a datalog database.
  os << os.Indent() << "class " << gClassName << "Interface(Protocol):\n";
  os.PushIndent();

  // Emit one method per received message that adds the message data into
  // the aggregate output message.
  for (auto message : messages) {
    if (message.IsPublished()) {
      continue;
    }
    os << os.Indent() << "def " << message.Name() << '_' << message.Arity()
       << "(self";

    for (auto param : message.Parameters()) {
      os << ", " << param.Name() << ": " << TypeName(module, param.Type());
    }
    os << "):\n";
    os.PushIndent();
    os << os.Indent() << "...\n\n";
    os.PopIndent();
  }
  os.PopIndent();
  os << '\n';

  // Implements the `DatabaseLog` protocol, and aggregates all messages into
  // a single `DatabaseOutputMessage`.
  os << os.Indent() << "class " << gClassName << "OutputMessageAggregator:\n";
  os.PushIndent();

  os << os.Indent() << "def __init__(self, db: " << gClassName
     << "Interface):\n";
  os.PushIndent();
  os << os.Indent() << "self._db: Final[" << gClassName << "Interface] = db\n"
     << os.Indent() << "self._num_msgs: int = 0\n"
     << os.Indent() << "self._msg: " << gClassName << "OutputMessage = "
     << gClassName << "OutputMessage()\n\n";
  os.PopIndent();

  // Emit one method per published message that adds the message data into
  // the aggregate output message.
  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }
    os << os.Indent() << "def " << message.Name() << '_' << message.Arity()
       << "(self";

    for (auto param : message.Parameters()) {
      os << ", " << param.Name() << ": " << TypeName(module, param.Type());
    }

    os << ", _added: bool):\n";
    os.PushIndent();
    os << os.Indent() << "if self._msg." << message.Name() << '_'
       << message.Arity() << " is None:\n";
    os.PushIndent();
    os << os.Indent() << "self._msg." << message.Name() << '_'
       << message.Arity() << " = []\n";
    os.PopIndent();
    os << os.Indent() << "self._msg." << message.Name() << '_'
       << message.Arity() << ".append((";
    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << param.Name();
      sep = ", ";
    }
    os << sep << "_added))\n"
       << os.Indent() << "self._num_msgs += 1\n\n";
    os.PopIndent();
  }

  // Emit a method that returns `None` if no messages were published by Datalog,
  // or emits an aggregated message representing all published messages since
  // the last time we asked.
  os << os.Indent() << "def aggregate(self) -> Optional[" << gClassName
     << "OutputMessage]:\n";
  os.PushIndent();
  os << os.Indent() << "if not self._num_msgs:\n";
  os.PushIndent();
  os << os.Indent() << "return None\n";
  os.PopIndent();
  os << os.Indent() << "self._num_msgs = 0\n"
     << os.Indent() << "msg = self._msg\n"
     << os.Indent() << "self._msg = " << gClassName << "OutputMessage()\n"
     << os.Indent() << "return msg\n\n";
  os.PopIndent();
  os << os.Indent() << "def __len__(self) -> int:\n";
  os.PushIndent();
  os << os.Indent() << "return self._num_msgs\n\n";
  os.PopIndent();
  os.PopIndent();
}

}  // namespace hyde
