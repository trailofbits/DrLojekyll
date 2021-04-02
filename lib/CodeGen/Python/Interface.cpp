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
     << "# flake8: noqa\n"  // Disable Flake8 linting.
     << "# fmt: off\n\n"  // Disable Black auto-formatting.
     << "from __future__ import annotations\n"
     << "from dataclasses import dataclass\n"
     << "from typing import Final, Iterator, List, Optional, Tuple\n"
     << "try:\n";
  os.PushIndent();
  os << os.Indent() << "from typing import Protocol\n";
  os.PopIndent();
  os << "except ImportError:\n";
  os.PushIndent();
  os << os.Indent() << "from typing_extensions import Protocol  # type: ignore\n\n\n";
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
       << "(self, vector: List[";

    if (1u < message.Arity()) {
      os << "Tuple[";
    }

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    if (1u < message.Arity() || message.IsDifferential()) {
      os << "]";
    }

    os << ']';

    if (message.IsDifferential()) {
      os << ", added: bool";
    }

    os << "):\n";
    os.PushIndent();
    os << os.Indent() << "...\n\n";
    os.PopIndent();
  }
  os.PopIndent();
  os << '\n';

  // Input messages to datalog.
  os << os.Indent() << "@dataclass\n"
     << os.Indent() << "class " << gClassName << "InputMessage:\n";

  os.PushIndent();

  for (auto message : messages) {
    if (message.IsPublished()) {
      continue;
    }

    os << os.Indent() << "_add_" << message.Name() << '_' << message.Arity()
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
    os << "]] = None\n";

    if (!message.IsDifferential()) {
      continue;
    }

    os << os.Indent() << "_rem_" << message.Name() << '_' << message.Arity()
       << ": Optional[List[";
    if (1u < message.Arity()) {
      os << "Tuple[";
    }

    sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    if (1u < message.Arity()) {
      os << ']';
    }
    os << "]] = None\n";
  }
  os << '\n';

  // Emit a method that will apply everything in this data class to an
  // instance of the database. Returns the number of processed messages.
  os << os.Indent() << "def apply(self, db: " << gClassName
     << "Interface) -> int:\n";
  os.PushIndent();
  os << os.Indent() << "num_messages: int = 0\n";
  for (auto message : messages) {
    if (message.IsPublished()) {
      continue;
    }
    os << os.Indent() << "if self._add_" << message.Name() << '_'
       << message.Arity() << " is not None:\n";
    os.PushIndent();
    os << os.Indent() << "num_messages += len(self._add_" << message.Name() << '_'
       << message.Arity() << ")\n"
       << os.Indent() << "db." << message.Name() << '_' << message.Arity()
       << "(self._add_" << message.Name() << '_' << message.Arity();
    if (message.IsDifferential()) {
      os << ", True";
    }
    os << ")\n";
    os.PopIndent();

    if (!message.IsDifferential()) {
      continue;
    }

    os << os.Indent() << "if self._rem_" << message.Name() << '_'
       << message.Arity() << " is not None:\n";
    os.PushIndent();
    os << os.Indent() << "num_messages += len(self._rem_" << message.Name() << '_'
       << message.Arity() << ")\n"
       << os.Indent() << "db." << message.Name() << '_' << message.Arity()
       << "(self._rem_" << message.Name() << '_' << message.Arity() << ", False)\n";
    os.PopIndent();
  }
  os << os.Indent() << "return num_messages\n";
  os.PopIndent();
  os.PopIndent();
  os << "\n\n";

  // Implements a class that can build input messages.
  os << os.Indent() << "class " << gClassName << "InputMessageProducer:\n";
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

    os << os.Indent() << "def produce_" << message.Name() << '_'
       << message.Arity() << "(self";

    for (auto param : message.Parameters()) {
      os << ", " << param.Name() << ": " << TypeName(module, param.Type());
    }

    if (message.IsDifferential()) {
      os << ", _added: bool";
    }

    os << "):\n";
    os.PushIndent();
    os << os.Indent() << "self._num_msgs += 1\n";

    if (!message.IsDifferential()) {
      os << os.Indent() << "_added = True\n";
    }

    // Differential, need to select among add/remove.

    os << os.Indent() << "if _added:\n";
    os.PushIndent();
    os << os.Indent() << "_msgs = self._msg._add_" << message.Name() << '_'
       << message.Arity() << '\n'
       << os.Indent() << "if _msgs is None:\n";
    os.PushIndent();
    os << os.Indent() << "_msgs = []\n"
       << os.Indent() << "self._msg._add_" << message.Name() << '_'
       << message.Arity() << " = _msgs\n";
    os.PopIndent();
    os.PopIndent();

    if (message.IsDifferential()) {
      os << os.Indent() << "else:\n";
      os.PushIndent();
      os << os.Indent() << "_msgs = self._msg._rem_" << message.Name() << '_'
         << message.Arity() << '\n'
         << os.Indent() << "if _msgs is None:\n";
      os.PushIndent();
      os << os.Indent() << "_msgs = []\n"
         << os.Indent() << "self._msg._rem_" << message.Name() << '_'
         << message.Arity() << " = _msgs\n";
      os.PopIndent();
      os.PopIndent();
    }

    os << os.Indent() << "_msgs.append(";
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

    os << ")  # type: ignore\n\n";
    os.PopIndent();
  }

  // Emit a method that returns `None` if no messages were published by Datalog,
  // or emits an aggregated message representing all published messages since
  // the last time we asked.
  os << os.Indent() << "def produce(self) -> Optional[" << gClassName
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

  // First, make the backing storage for the messages.
  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    empty = false;

    os << os.Indent() << "_add_" << message.Name() << '_' << message.Arity()
       << ": Optional[List[";

    if (1 < message.Arity()) {
      os << "Tuple[";
    }

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    if (1 < message.Arity()) {
      os << "]";
    }

    os << "]] = None\n";

    if (!message.IsDifferential()) {
      continue;
    }

    os << os.Indent() << "_rem_" << message.Name() << '_' << message.Arity()
       << ": Optional[List[";

    if (1 < message.Arity()) {
      os << "Tuple[";
    }

    sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }

    if (1 < message.Arity()) {
      os << "]";
    }

    os << "]] = None\n";
  }

  // Then, expose them via properties / accessor functions.
  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    // In the differential case, we have a function giving access to one of the
    // internal lists, based on the parameter of `True` (added) or `False`
    // (removed).
    if (message.IsDifferential()) {
      os << os.Indent() << "def " << message.Name() << '_' << message.Arity()
         << "(self, _added: bool) -> Iterator[";

      if (1 < message.Arity()) {
        os << "Tuple[";
      }

      auto sep = "";
      for (auto param : message.Parameters()) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }

      if (1 < message.Arity()) {
        os << "]";
      }

      os << "]:\n";
      os.PushIndent();
      os << os.Indent() << "if _added:\n";

      os.PushIndent();
      os << os.Indent() << "if self._add_" << message.Name() << '_'
         << message.Arity() << " is not None:\n";
      os.PushIndent();
      os << os.Indent() << "for _row in self._add_" << message.Name() << '_'
         << message.Arity() << ":\n";
      os.PushIndent();
      os << os.Indent() << "yield _row\n";
      os.PopIndent();
      os.PopIndent();
      os.PopIndent();

      os << os.Indent() << "else:\n";
      os.PushIndent();
      os << os.Indent() << "if self._rem_" << message.Name() << '_'
         << message.Arity() << " is not None:\n";
      os.PushIndent();
      os << os.Indent() << "for _row in self._rem_" << message.Name() << '_'
         << message.Arity() << ":\n";
      os.PushIndent();
      os << os.Indent() << "yield _row\n";
      os.PopIndent();
      os.PopIndent();

      os.PopIndent();
      os.PopIndent();

    // In the non-differential case, we have a property giving access to the
    // private field, in terms of an iterator.
    } else {
      os << os.Indent() << "@property\n"
         << os.Indent() << "def " << message.Name() << '_' << message.Arity()
         << "(self) -> Iterator[";

      if (1 < message.Arity()) {
        os << "Tuple[";
      }

      auto sep = "";
      for (auto param : message.Parameters()) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }

      if (1 < message.Arity()) {
        os << "]";
      }

      os << "]:\n";
      os.PushIndent();
      os << os.Indent() << "if self._add_" << message.Name() << '_'
         << message.Arity() << " is not None:\n";
      os.PushIndent();
      os << os.Indent() << "for _row in self._add_" << message.Name() << '_'
         << message.Arity() << ":\n";
      os.PushIndent();
      os << os.Indent() << "yield _row\n";
      os.PopIndent();
      os.PopIndent();
      os.PopIndent();
    }
  }

  if (empty) {
    os << os.Indent() << "pass\n";
  }

  os.PopIndent();
  os << "\n\n";

  // Implements the `DatabaseLog` protocol, and aggregates all messages into
  // a single `DatabaseOutputMessage`.
  os << os.Indent() << "class " << gClassName << "OutputMessageProducer:\n";
  os.PushIndent();

  os << os.Indent() << "def __init__(self):\n";
  os.PushIndent();
  os << os.Indent() << "self._num_msgs: int = 0\n"
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

    if (message.IsDifferential()) {
      os << ", _added: bool";
    }

    os << "):\n";
    os.PushIndent();

    if (!message.IsDifferential()) {
      os << os.Indent() << "_added = True\n";
    }

    os << os.Indent() << "if _added:\n";
    os.PushIndent();
    os << os.Indent() << "_msgs = self._msg._add_" << message.Name() << '_'
       << message.Arity() << '\n'
       << os.Indent() << "if _msgs is None:\n";
    os.PushIndent();
    os << os.Indent() << "_msgs = []\n"
       << os.Indent() << "self._msg._add_" << message.Name() << '_'
       << message.Arity() << " = _msgs\n";
    os.PopIndent();
    os.PopIndent();

    if (message.IsDifferential()) {
      os << os.Indent() << "else:\n";
      os.PushIndent();
      os << os.Indent() << "_msgs = self._msg._rem_" << message.Name() << '_'
         << message.Arity() << '\n'
         << os.Indent() << "if _msgs is None:\n";
      os.PushIndent();
      os << os.Indent() << "_msgs = []\n"
         << os.Indent() << "self._msg._rem_" << message.Name() << '_'
         << message.Arity() << " = _msgs\n";
      os.PopIndent();
      os.PopIndent();
    }

    os << os.Indent() << "_msgs.append(";
    if (1 < message.Arity()) {
      os << "(";
    }

    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << param.Name();
      sep = ", ";
    }
    if (1 < message.Arity()) {
      os << ")";
    }
    os << ")\n";

    os << os.Indent() << "self._num_msgs += 1\n\n";
    os.PopIndent();
  }

  // Emit a method that returns `None` if no messages were published by Datalog,
  // or emits an aggregated message representing all published messages since
  // the last time we asked.
  os << os.Indent() << "def produce(self) -> Optional[" << gClassName
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
  os << os.Indent() << "return self._num_msgs\n\n\n";
  os.PopIndent();
  os.PopIndent();

  os << os.Indent() << "class " << gClassName << "OutputMessageConsumer:\n";
  os.PushIndent();
  os << os.Indent() << "def consume(self, msg: " << gClassName
     << "OutputMessage):\n";
  os.PushIndent();
  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }
    os << os.Indent() << "if msg._add_" << message.Name() << '_' << message.Arity()
       << " is not None:\n";
    os.PushIndent();
    os << os.Indent() << "for _row in msg._add_" << message.Name() << '_'
       << message.Arity() << ":\n";
    os.PushIndent();
    os << os.Indent() << "self.consume_" << message.Name() << '_'
       << message.Arity() << "(";

    if (1 == message.Arity()) {
      os << "_row";
    } else {
      auto sep = "";
      for (auto i = 0u; i < message.Arity(); ++i) {
        os << sep << "_row[" << i << ']';
        sep = ", ";
      }
    }

    if (message.IsDifferential()) {
      os << ", True";
    }

    os << ")\n";
    os.PopIndent();
    os.PopIndent();

    if (!message.IsDifferential()) {
      continue;
    }

    os << os.Indent() << "if msg._rem_" << message.Name() << '_' << message.Arity()
       << " is not None:\n";
    os.PushIndent();
    os << os.Indent() << "for _row in msg._rem_" << message.Name() << '_'
       << message.Arity() << ":\n";
    os.PushIndent();
    os << os.Indent() << "self.consume_" << message.Name() << '_'
       << message.Arity() << "(";

    if (1 == message.Arity()) {
      os << "_row";
    } else {
      auto sep = "";
      for (auto i = 0u; i < message.Arity(); ++i) {
        os << sep << "_row[" << i << ']';
        sep = ", ";
      }
    }

    os << ", False)\n";
    os.PopIndent();
    os.PopIndent();
  }
  os << os.Indent() << "return\n\n";
  os.PopIndent();

  for (auto message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << os.Indent() << "def consume_" << message.Name() << '_'
       << message.Arity() << "(self";

    for (auto param : message.Parameters()) {
      os << ", " << param.Name() << ": " << TypeName(module, param.Type());
    }

    if (message.IsDifferential()) {
      os << ", _added: bool";
    }

    os << "):\n";
    os.PushIndent();
    os << os.Indent() << "pass\n\n";
    os.PopIndent();
  }
  os.PopIndent();
}

}  // namespace hyde
