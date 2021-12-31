// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <cassert>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "Util.h"


namespace hyde {
namespace python {

// Emits Python code for the given program to `os`.
void GenerateInterfaceCode(const Program &program, OutputStream &os) {
  const auto module = program.ParsedModule();
  const auto db_name = module.DatabaseName();
  const auto queries = Queries(module);
  const auto messages = Messages(module);

  std::string file_name = "datalog";
  std::string ns_name;
  std::string ns_name_prefix;
  if (db_name) {
    file_name = db_name->FileName();
    ns_name = db_name->NamespaceName(Language::kPython);
    ns_name_prefix = ns_name + ".";
  }

  auto has_inputs = false;
  auto has_outputs = false;
  auto has_removed_inputs = false;
  auto has_removed_outputs = false;
  for (auto message : messages) {
    if (message.IsDifferential()) {
      if (message.IsPublished()) {
        has_removed_outputs = true;
      } else if (message.IsReceived()) {
        has_removed_inputs = true;
      } else {
        assert(false);
      }
    } else if (message.IsPublished()) {
      has_outputs = true;
    } else if (message.IsReceived()) {
      has_inputs = true;
    }
  }

  os << "# Auto-generated file\n\n"
     << "from __future__ import annotations\n\n"
     << "import grpc\n"
     << "import flatbuffers\n\n"
     << "from dataclasses import dataclass\n"
     << "from typing import Final, Iterator, List, Optional\n"
     << "from ." << file_name << "_grpc_fb import DatalogStub\n"
     << "from .InputMessage import InputMessageT as InputMessage\n"
     << "from .OutputMessage import OutputMessageT as OutputMessage\n"
     << "from .Client import ClientT as Client\n"
     << "from .AddedInputMessage import AddedInputMessageT as AddedInputMessage\n";

  if (has_outputs) {
    os << "from .AddedOutputMessage import AddedOutputMessageT as AddedOutputMessage\n";
  }
  if (has_removed_outputs) {
    os << "from .RemovedOutputMessage import RemovedOutputMessageT as RemovedOutputMessage\n";
  }

  if (has_removed_inputs) {
    os << "from .RemovedInputMessage import RemovedInputMessageT as RemovedInputMessage\n";
  }

  // Import each message builder object with a nice name.
  for (ParsedMessage message : messages) {
    const auto name = message.Name();
    const auto arity = message.Arity();
    os << "from .Message_" << name << "_" << arity
       << " import Message_" << name << "_" << arity
       << "T as " << name << "_" << arity << '\n';
  }

  // Import each query builder object with a nice name.
  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    const auto name = query.Name();
    const auto arity = query.Arity();

    if (decl.IsFirstDeclaration()) {
      os << "from ." << name << "_" << arity
         << " import " << name << "_" << arity
         << "T as " << name << "_" << arity << '\n';
    }

    auto bp = decl.BindingPattern();
    os << "from ." << name << "_" << bp
       << " import " << name << "_" << bp
       << "T as " << name << "_" << bp << '\n';
  }

  os << '\n';

  os << os.Indent() << "class InputMessageBuilder:\n";
  os.PushIndent();
  if (has_inputs) {

    os << os.Indent() << "_msg: InputMessage\n"
       << os.Indent() << "_added_msg: Optional[AddedInputMessage]\n";
    if (has_removed_inputs) {
      os << os.Indent() << "_removed_msg: Optional[RemovedInputMessage]\n";
    }
    os << '\n'
       << os.Indent() << "def __init__(self):\n";
    os.PushIndent();
    os << os.Indent() << "self._msg = InputMessage()\n"
       << os.Indent() << "self._added_msg = None\n";
    if (has_removed_inputs) {
      os << os.Indent() << "self._removed_msg = None\n";
    }
    os << "\n";
    os.PopIndent();

    os << os.Indent() << "def _added(self) -> AddedInputMessage:\n";
    os.PushIndent();
    os << os.Indent() << "if self._added_msg is None:\n";
    os.PushIndent();
    os << os.Indent() << "self._added_msg = AddedInputMessage()\n"
       << os.Indent() << "self._msg.added = self._added_msg\n";
    os.PopIndent();
    os << os.Indent() << "return self._added_msg\n\n";
    os.PopIndent();  // _added

    if (has_removed_inputs) {
      os << os.Indent() << "def _removed(self) -> RemovedInputMessage:\n";
      os.PushIndent();
      os << os.Indent() << "if self._removed_msg is None:\n";
      os.PushIndent();
      os << os.Indent() << "self._removed_msg = AddedInputMessage()\n"
         << os.Indent() << "self._msg.removed = self._added_msg\n";
      os.PopIndent();
      os << os.Indent() << "return self._removed_msg\n\n";
      os.PopIndent();  // _removed
    }

    auto do_message = [&] (ParsedMessage message, const char *prefix, const char *method) {
      const ParsedDeclaration decl(message);
      const auto name = message.Name();
      const auto arity = message.Arity();
      os << os.Indent() << "def " << prefix << name << '_' << arity << "(self";

      for (ParsedParameter param : decl.Parameters()) {
        os << ", " << param.Name() << ": " << TypeName(module, param.Type());
      }

      os << ") -> None:\n";
      os.PushIndent();
      os << os.Indent() << "ls = self." << method << "()\n"
         << os.Indent() << "if ls." << name << '_' << arity << " is None:\n";
      os.PushIndent();
      os << os.Indent() << "ls." << name << '_' << arity << " = []\n";
      os.PopIndent();
      os << os.Indent() << "msg = " << name << '_' << arity << "()\n";
      for (ParsedParameter param : decl.Parameters()) {
        os << os.Indent() << "msg." << param.Name() << " = " << param.Name() << '\n';
      }
      os << os.Indent() << "ls." << name << '_' << arity << ".append(msg)\n\n";
      os.PopIndent();
    };

    for (ParsedMessage message : messages) {
      if (!message.IsReceived()) {
        continue;
      }

      do_message(message, "produce_", "_added");

      if (message.IsDifferential()) {
        do_message(message, "retract_", "_removed");
      }
    }
  } else {
    os << os.Indent() << "pass\n\n";
  }
  os.PopIndent();  // class InputMessageBuilder

  os << "class OutputMessageConsumer:\n";
  os.PushIndent();
  os << os.Indent() << "def begin(self, db: 'Datalog') -> None:\n";
  os.PushIndent();
  os << os.Indent() << "pass\n\n";
  os.PopIndent();

  os << os.Indent() << "def end(self, db: 'Datalog') -> None:\n";
  os.PushIndent();
  os << os.Indent() << "pass\n\n";
  os.PopIndent();

  if (has_outputs) {
    for (ParsedMessage message : messages) {
      if (!message.IsPublished()) {
        continue;
      }

      const ParsedDeclaration decl(message);
      const auto name = message.Name();
      const auto arity = message.Arity();
      os << os.Indent() << "def consume_" << name << '_' << arity
         << "(self, db: 'Datalog'";

      for (ParsedParameter param : decl.Parameters()) {
        os << ", " << param.Name() << ": " << TypeName(module, param.Type());
      }

      if (message.IsDifferential()) {
        os << ", added: bool";
      }

      os << "):\n";
      os.PushIndent();
      os << os.Indent() << "pass\n\n";
      os.PopIndent();
    }
  }

  os << os.Indent() << "def consume(self, db: 'Datalog', output: OutputMessage):\n";
  os.PushIndent();
  os << os.Indent() << "self.begin(db)\n";
  if (has_outputs) {
    auto i = 0;

    auto do_message = [&] (ParsedMessage message, const char *list, const char *added) {
      const ParsedDeclaration decl(message);
      const auto name = message.Name();
      const auto arity = message.Arity();
      os << os.Indent() << "if output." << list << '.' << name << '_' << arity << " is not None:\n";
      os.PushIndent();
      os << os.Indent() << "for m" << i << " in output." << list << '.' << name << '_' << arity << ":\n";
      os.PushIndent();
      os << os.Indent() << "self.consume_" << name << '_' << arity << "(db";
      for (ParsedParameter param : decl.Parameters()) {
        os << ", m" << i << "." << param.Name();
      }
      if (message.IsDifferential()) {
        os << ", " << added;
      }
      os << ")\n";
      os.PopIndent();  // for
      os.PopIndent();  // if
    };

    os << os.Indent() << "if output.added is not None:\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsPublished()) {
        do_message(message, "added", "True");
        ++i;
      }
    }
    os.PopIndent();  // if output.added

    if (has_removed_outputs) {
      os << os.Indent() << "if output.removed is not None:\n";
      os.PushIndent();
      for (ParsedMessage message : messages) {
        if (message.IsPublished()) {
          do_message(message, "removed", "False");
          ++i;
        }
      }
      os.PopIndent();  // if output.removed
    }
  }
  os << os.Indent() << "self.end(db)\n\n";
  os.PopIndent();  // consume
  os.PopIndent();  // class OutputMesssageConsumer

  os << "class Datalog:\n";
  os.PushIndent();
  os << os.Indent() << "_stub: DatalogStub\n\n"
     << os.Indent() << "def __init__(self, client_name: str, channel: grpc.Channel):\n";
  os.PushIndent();
  os << os.Indent() << "self._name = client_name\n"
     << os.Indent() << "self._stub = DatalogStub(channel)\n\n";
  os.PopIndent();  // __init__

  os << os.Indent() << "def produce(self, builder: InputMessageBuilder) -> None:\n";
  os.PushIndent();
  if (has_inputs) {
    os << os.Indent() << "message_builder = flatbuffers.Builder(0)\n"
       << os.Indent() << "message = builder._msg\n"
       << os.Indent() << "builder._msg = InputMessage()\n"
       << os.Indent() << "builder._added_msg = None\n";
    if (has_removed_inputs) {
      os << os.Indent() << "builder._removed_msg = None\n";
    }
    os << os.Indent() << "offset = message.Pack(message_builder)\n"
       << os.Indent() << "message_builder.Finish(offset)\n"
       << os.Indent() << "self._stub.Publish(bytes(message_builder.Output()))\n\n";
  } else {
    os << os.Indent() << "pass\n\n";
  }
  os.PopIndent();  // publish

  // Emit each of the queries.
  for (ParsedQuery query : queries) {
    const ParsedDeclaration decl(query);
    const auto name = query.Name();
    const auto arity = query.Arity();
    const auto bp = decl.BindingPattern();
    os << os.Indent() << "def " << name << '_' << bp << "(self";

    auto has_bound = false;
    auto has_free = false;
    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << ", " << param.Name() << ": " << TypeName(module, param.Type());
        has_bound = true;
      } else {
        has_free = true;
      }
    }

    os << ")";
    if (has_free) {
      os << " -> Iterator[" << name << '_' << arity << "]:\n";
    } else {
      os << " -> Optional[" << name << '_' << arity << "]:\n";
    }
    os.PushIndent();
    os << os.Indent() << "req = " << name << '_' << bp << "()\n";
    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << os.Indent() << "req." << param.Name() << " = " << param.Name() << '\n';
      }
    }
    os << os.Indent() << "message_builder = flatbuffers.Builder(0)\n"
       << os.Indent() << "offset = req.Pack(message_builder)\n"
       << os.Indent() << "message_builder.Finish(offset)\n"
       << os.Indent() << "buff = bytes(message_builder.Output())\n"
       << os.Indent() << "resp = self._stub.Query_" << name << '_' << bp << "(buff)\n";

    // It's a `_MultiThreadedRendezvous`.
    if (has_free) {
      os << os.Indent() << "for resp_buff in resp:\n";
      os.PushIndent();
      os << os.Indent() << "yield " << name << '_' << arity << ".InitFromBuf(resp_buff, 0)\n";
      os.PopIndent();

      os << os.Indent() << "del resp\n\n";
    } else {

    }

    os.PopIndent();
    os << '\n';
  }

  os << os.Indent() << "def consume(self, consumer: OutputMessageConsumer) -> None:\n";
  os.PushIndent();
  os << os.Indent() << "message_builder = flatbuffer.Builder(0)\n"
     << os.Indent() << "message = Client()\n"
     << os.Indent() << "message.name = self._name\n"
     << os.Indent() << "offset = message.Pack(message_builder)\n"
     << os.Indent() << "message_builder.Finish(offset)\n"
     << os.Indent() << "buff = bytes(message_builder.Output())\n"
     << os.Indent() << "while True:\n";
  os.PushIndent();
  os << os.Indent() << "x = self._stub.Subscribe(buff)\n"
     << os.Indent() << "print(x.__class__)\n"
     << os.Indent() << "print(dir(x))\n"
     << os.Indent() << "print(x)\n";

  os.PopIndent();  // while
  os.PopIndent();  // consume

  os.PopIndent();  // class Datalog
  os << "\n";
}

}  // namespace python
}  // namespace hyde
