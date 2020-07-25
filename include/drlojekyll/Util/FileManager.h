// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

// TODO(pag): Eventually get rid of all of this in favor of std::fileystem.

#include <functional>
#include <memory>
#include <string_view>
#include <system_error>
#include <utility>

namespace hyde {

class FileEntry;
class FileManager;
class FileManagerImpl;

enum class PathKind {
  kWindows,
  kPosix,
#if defined(WIN32) || defined(_WIN32) || \
    defined(__WIN32) && !defined(__CYGWIN__)
  kDefault = kWindows
#else
  kDefault = kPosix
#endif
};

// Represents a path to a file or directory in a file system. The path
// abstraction is more than just a wrapper around strings. The file system
// backs every plausible path with an actual file entry. Therefore, path
// instances must not exceed the lifetime of the `FileManager` from whence
// they were created.
class Path {
 public:
  inline Path(const Path &that) : entry(that.entry) {}

  inline Path(Path &&that) noexcept : entry(that.entry) {}

  Path(const FileManager &fs, std::string_view path);
  Path(const FileManager &fs);  // Makes the root path.

  // Switch the path to something new.
  void Reset(std::string_view path);

  // Return the full path.
  std::string_view FullPath(void) const;

  // Return the real path or full path.
  std::string_view RealOrFullPath(void) const;

  // Replace this path with its real path form, if possible.
  std::error_code Realize(void);

  // Figure out the real path of this file.
  std::error_code RealPath(std::string_view *out_real_path) const;

  // Get the size of the file.
  std::error_code FileSize(size_t *out_size) const;

  // Extend the path in place.
  void Push(std::string_view part);

  // Pop the last part off of the path.
  bool Pop(void);

  // Append a part to the path, returning a new path.
  Path Join(std::string_view part) const;

  // Return the last component of the path.
  std::string_view BaseName(void) const;

  // Return the path associated with the directory containing the currently
  // referenced path.
  Path DirName(void) const;

  bool Exists(void) const;
  bool IsFile(void) const;
  bool IsDirectory(void) const;
  bool IsExecutable(void) const;

  inline Path &operator=(const Path &that) {
    entry = that.entry;
    return *this;
  }

  inline Path &operator=(Path &&that) noexcept {
    entry = that.entry;
    return *this;
  }

  // Equivalence comparison will operate on the real path (if any) of this
  // path.
  bool operator==(Path that) const;

  bool operator!=(Path that) const {
    return !(*this == that);
  }

  // Returns a hash of this path. This is a hash of the `RealOrFullPath` of
  // this entry. It is cached.
  uintptr_t Hash(void) const;

 private:
  friend class FileManager;
  friend class FileManagerImpl;

  Path(void) = delete;
  Path(FileEntry *entry_);

  std::error_code ComputeRealPath(void) const;

  FileEntry *entry;
};

// Abstractions on top of a file system.
class FileManager {
 public:
  ~FileManager(void);

  inline FileManager(void) : FileManager(PathKind::kDefault) {}

  explicit FileManager(PathKind path_kind);
  FileManager(const FileManager &that);
  FileManager(FileManager &&that) noexcept;

  FileManager &operator=(const FileManager &that);
  FileManager &operator=(FileManager &&that) noexcept;

  // Get the current working directory.
  Path CurrentDirectory(void) const;

  // Enter into the directory `path`, and push it onto the working directory
  // stack. If there's an error then nothing is pushed on the stack.
  std::error_code PushDirectory(Path path) const;

  // Pops the top directory off the stack, and changes the current working
  // directory to the next one down off the stack. If the latter operation
  // fails then the program will abort.
  void PopDirectory(void) const;

  // Apply `cb` to every path in the directory `dir`. Returns an error code if
  // `dir` is not a directory. If `cb` returns `false` then the iteration is
  // stopped.
  static std::error_code ForEachPathInDirectory(Path dir,
                                                std::function<bool(Path)> cb);

  // Create a directory.
  static std::error_code CreateDirectory(Path path);

  // Remove a file.
  static std::error_code RemoveFile(Path path);

 private:
  friend class FileEntry;
  friend class Path;

  std::shared_ptr<FileManagerImpl> impl;
};

}  // namespace hyde
namespace std {

// Specialize `std::hash` so that we can put paths into hash tables and sets.
template <>
struct hash<hyde::Path> {
 public:
  typedef uintptr_t result_type;
  typedef ::hyde::Path argument_type;
  inline result_type operator()(argument_type p) const {
    return p.Hash();
  }
};
}  // namespace std
