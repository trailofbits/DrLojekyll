// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Util/FileManager.h>

#include <cassert>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <vector>

#if !defined(WIN32)
#  include <dirent.h>
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

#if defined(__APPLE__)
#  include <sys/syslimits.h>
#elif defined(__linux__)
#  include <linux/limits.h>
#endif

#if defined(_MAX_PATH) && !defined(PATH_MAX)
#  define PATH_MAX _MAX_PATH
#endif

#if defined(MAXPATHLEN) && !defined(PATH_MAX)
#  define PATH_MAX MAXPATHLEN
#endif

#if !defined(PATH_MAX)
#  define PATH_MAX 4096
#endif

// Do not include windows.h, or its macros might end up shadowing our functions.
#if defined(WIN32)
namespace {
const uint32_t INVALID_FILE_ATTRIBUTES = static_cast<uint32_t>(-1);
const uint32_t ERROR_NO_MORE_FILES = 18U;
const uint32_t FILE_ATTRIBUTE_DIRECTORY = 16U;
const uint32_t INVALID_HANDLE_VALUE = static_cast<uint32_t>(-1);
const int MAX_PATH = 260;

struct FILETIME {
  uint32_t dwLowDateTime;
  uint32_t dwHighDateTime;
};

struct WIN32_FIND_DATA {
  uint32_t dwFileAttributes;
  FILETIME ftCreationTime;
  FILETIME ftLastAccessTime;
  FILETIME ftLastWriteTime;
  uint32_t nFileSizeHigh;
  uint32_t nFileSizeLow;
  uint32_t dwReserved0;
  uint32_t dwReserved1;
  char cFileName[MAX_PATH];
  char cAlternateFileName[14];
};

extern "C" uint32_t GetFullPathNameA(const char *file_name, uint32_t buff_len,
                                     char *buff, char **last_file_part_ptr);
extern "C" int CreateDirectoryA(const char *path_name,
                                void *security_attributes);
extern "C" uint32_t GetCurrentDirectoryA(uint32_t buffer_length, char *buffer);
extern "C" uint32_t GetFileAttributesA(const char *file_name);
extern "C" int CopyFileA(const char *existing_file_name,
                         const char *new_file_name, int file_if_exists);
extern "C" int CreateHardLinkA(const char *file_name,
                               const char *existing_file_name,
                               void *security_attributes);
extern "C" uint32_t FindFirstFileA(const char *file_name,
                                   WIN32_FIND_DATA *find_data);
extern "C" uint32_t FindNextFileA(uint32_t handle, WIN32_FIND_DATA *find_data);
extern "C" int FindClose(uint32_t handle);
extern "C" uint32_t GetLastError();
extern "C" int _chdir(const char *dir_name);

static int chdir(const char *dir_name) {
  return _chdir(dir_name);
}

static char *getcwd(char *buf, size_t size) {
  if (GetCurrentDirectoryA(static_cast<uint32_t>(size), buf) == 0) {
    return nullptr;
  }

  return buf;
}

static int mkdir(const char *pathname, uint16_t mode) {
  static_cast<void>(mode);

  if (CreateDirectoryA(pathname, nullptr) == 0) {
    return -1;
  }

  return 0;
}

}  // namespace

#endif

namespace hyde {
namespace {

static constexpr size_t kPathBuffSize = PATH_MAX * 2;

static thread_local std::unique_ptr<std::string> kEmptyStrPtr;
static constexpr std::hash<std::string_view> kHasher = {};

}  // namespace

// Implementation of `FileManager`.
class FileManagerImpl {
 public:
  FileManagerImpl(PathKind path_kind_);
  Path CurrentDirectory(void) const;
  std::error_code PushDirectory(Path path);
  std::error_code CreateDirectory(Path path);
  void PopDirectory(void);

 private:
  friend class Path;
  friend class FileEntry;
  friend class FileManager;

  FileManagerImpl(void) = delete;

  std::shared_ptr<FileManagerImpl> impl;

  FileEntry *GetEntryFor(std::string_view path);
  FileEntry *GetEntryFor(FileEntry *dir, std::string_view path);
  FileEntry *GetEntryFor(FileEntry *dir, size_t num_parts, bool is_absolute);
  size_t SplitPathBuff(void);

  std::string path_buff;
  std::vector<Path> path_stack;
  std::vector<std::string> path_parts;
  std::unique_ptr<FileEntry> root;
  const PathKind path_kind;
  const char path_separator;
};

// Represents an entry.
class FileEntry {
 public:
  FileEntry(void) = delete;

  explicit FileEntry(FileManagerImpl *fs_);
  FileEntry(FileManagerImpl *fs_, FileEntry *parent_);

  // As long as a path exists, so does a
  FileManagerImpl *const fs;
  FileEntry *parent;
  FileEntry *real_path_entry;
  size_t hash_code;
  std::error_code real_path_ec;

  std::string full_path;
  std::string_view name;
  std::unordered_map<std::string_view, std::unique_ptr<FileEntry>> children;

  FileEntry *GetOrAddChild(std::string_view child_name) {
    if (child_name.empty() || child_name == ".") {
      return this;

    } else if (child_name == "..") {
      return this->parent;
    }

    auto child_it = children.find(child_name);
    if (child_it != children.end()) {
      return child_it->second.get();
    }

    std::stringstream ss;
    if (this != parent) {
      ss << full_path;
    }
    ss << fs->path_separator << child_name;

    auto new_ent = new FileEntry(fs, this);
    new_ent->full_path = ss.str();

    std::string_view full_path_view = new_ent->full_path;
    new_ent->name =
        full_path_view.substr(new_ent->full_path.size() - child_name.size());

    children[new_ent->name].reset(new_ent);
    return new_ent;
  }
};

FileEntry::FileEntry(FileManagerImpl *fs_)
    : fs(fs_),
      parent(this),
      real_path_entry(nullptr),
      hash_code(0) {}

FileEntry::FileEntry(FileManagerImpl *fs_, FileEntry *parent_)
    : fs(fs_),
      parent(parent_),
      real_path_entry(nullptr),
      hash_code(0) {}

Path::Path(const FileManager &fs, std::string_view path)
    : Path(fs.impl->GetEntryFor(path)) {}

Path::Path(FileEntry *entry_) : entry(entry_) {}

// Makes the root path.
Path::Path(const FileManager &fs) : entry(fs.impl->root.get()) {}

// Switch the path to something new.
void Path::Reset(std::string_view path) {
  entry = entry->fs->GetEntryFor(path);
}

// Return the last component of the path as a `std::string`.
std::string_view Path::BaseName(void) const {
  if (entry == entry->parent) {
    if (!kEmptyStrPtr) {
      kEmptyStrPtr.reset(new std::string);
    }
    return *kEmptyStrPtr;

  } else {
    return entry->name;
  }
}

// Return the full path as a C string.
std::string_view Path::FullPath(void) const {
  return entry->full_path;
}

// Return the real path or full path as a `std::string`.
std::string_view Path::RealOrFullPath(void) const {
  if (ComputeRealPath()) {
    return entry->full_path;
  } else {
    return entry->real_path_entry->full_path;
  }
}

// Replace this path with its real path form, if possible.
std::error_code Path::Realize(void) {
  auto ec = ComputeRealPath();
  if (ec) {
    return ec;
  } else {
    entry = entry->real_path_entry;
    return std::error_code();
  }
}

std::error_code Path::ComputeRealPath(void) const {
  if (!entry->real_path_entry && !entry->real_path_ec) {
    entry->fs->path_buff.resize(0);
    entry->fs->path_buff.resize(entry->full_path.size() + kPathBuffSize - 1);

    auto full_path = entry->full_path.c_str();
    auto path_buff = &(entry->fs->path_buff[0]);

#ifdef WIN32
    auto path_buff_size = entry->fs->path_buff.size();
    auto ret =
        GetFullPathNameA(full_path, path_buff_size - 1, path_buff, nullptr);
    auto err = GetLastError();
#else
    auto ret = realpath(full_path, path_buff);
    auto err = errno;
#endif

    if (!ret) {
      entry->real_path_ec = std::error_code(err, std::system_category());

    } else {
      auto real_path_entry = entry->fs->GetEntryFor(path_buff);

      // Make sure the path entry points to itself as being a real path.
      real_path_entry->real_path_entry = real_path_entry;

      // Make sure this path's entry is also pointing to the real path
      // entry.
      entry->real_path_entry = real_path_entry;
      entry->real_path_ec = std::error_code();
    }
  }

  return entry->real_path_ec;
}

// Figure out the real path of this file.
std::error_code Path::RealPath(std::string_view *real_path_out) const {
  auto ec = ComputeRealPath();
  if (!ec && real_path_out) {
    *real_path_out = entry->real_path_entry->full_path;
  }
  return ec;
}

// Get the size of the file.
std::error_code Path::FileSize(size_t *out_size) const {
  auto ec = ComputeRealPath();
  if (ec) {
    return ec;
  }

  const auto real_path = entry->real_path_entry->full_path.c_str();

#ifdef WIN32
  WIN32_FIND_DATA find_data = {};
  auto handle = FindFirstFileA(real_path, &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    auto err = GetLastError();
    return std::error_code(err, std::system_category());
  }
  FindClose(handle);

  uint64_t high = find_data.nFileSizeHigh;
  uint64_t low = find_data.nFileSizeLow;
  uint64_t high_low = (high << 32ull) | low;
  if (out_size) {
    *out_size = static_cast<size_t>(high_low);
  }
  return std::error_code();

#else
  struct stat file_info = {};
  if (stat(real_path, &file_info)) {
    return std::error_code(errno, std::system_category());
  }

  if (S_ISDIR(file_info.st_mode)) {
    return std::make_error_code(std::errc::is_a_directory);
  } else if (!S_ISREG(file_info.st_mode)) {
    return std::make_error_code(std::errc::not_supported);
  } else if (0 > file_info.st_size) {
    return std::make_error_code(std::errc::value_too_large);
  }

  if (out_size) {
    *out_size = static_cast<size_t>(file_info.st_size);
  }
  return std::error_code();

#endif
}

void Path::Push(std::string_view part) {
  entry = entry->fs->GetEntryFor(entry, part);
}

// Pop the last part off of the path.
bool Path::Pop(void) {
  if (entry == entry->parent) {
    return false;
  } else {
    entry = entry->parent;
    return true;
  }
}

Path Path::Join(std::string_view part) const {
  return entry->fs->GetEntryFor(entry, part);
}

// Pop the last part off of the path.
Path Path::DirName(void) const {
  return entry->parent;
}

bool Path::Exists(void) const {
  if (ComputeRealPath()) {
    return false;
  }

  const auto real_path = entry->real_path_entry->full_path.c_str();

#ifdef WIN32
  WIN32_FIND_DATA find_data = {};
  auto handle = FindFirstFileA(real_path, &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  } else {
    FindClose(handle);
    return true;
  }

#else
  struct stat file_info = {};
  if (stat(real_path, &file_info)) {
    return false;
  } else {
    return true;
  }
#endif
}

bool Path::IsExecutable(void) const {
  if (ComputeRealPath()) {
    return false;
  }

  const auto real_path = entry->real_path_entry->full_path.c_str();

#ifdef WIN32
  WIN32_FIND_DATA find_data = {};
  auto handle = FindFirstFileA(real_path, &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  } else {
    FindClose(handle);
#  error "TODO Path::IsExecutable"
    return true;
  }

#else
  struct stat file_info = {};
  if (stat(real_path, &file_info)) {
    return false;
  } else {
    return 0 != (file_info.st_mode & S_IXUSR);
  }
#endif
}

bool Path::IsFile(void) const {
  if (ComputeRealPath()) {
    return false;
  }

  const auto real_path = entry->real_path_entry->full_path.c_str();

#ifdef WIN32
  WIN32_FIND_DATA find_data = {};
  auto handle = FindFirstFileA(real_path, &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  } else {
    FindClose(handle);
#  error "TODO Path::IsFile"
    return true;
  }

#else
  struct stat file_info = {};
  if (stat(real_path, &file_info)) {
    return false;
  } else {
    return S_ISREG(file_info.st_mode);
  }
#endif
}

bool Path::IsDirectory(void) const {
  if (ComputeRealPath()) {
    return false;
  }

  const auto real_path = entry->real_path_entry->full_path.c_str();

#ifdef WIN32
  WIN32_FIND_DATA find_data = {};
  auto handle = FindFirstFileA(real_path, &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  } else {
    FindClose(handle);
#  error "TODO Path::IsDirectory"
    return true;
  }

#else
  struct stat file_info = {};
  if (stat(real_path, &file_info)) {
    return false;
  } else {
    return S_ISDIR(file_info.st_mode);
  }
#endif
}

// Compares two paths for equality.
bool Path::operator==(Path that) const {
  if (entry == that.entry) {
    return true;

  // NOTE(pag): `Hash` computes `real_path_entry`.
  } else {
    return Hash() == that.Hash() &&
           entry->real_path_entry == that.entry->real_path_entry;
  }
}

uintptr_t Path::Hash(void) const {
  if (entry->hash_code) {
    return entry->hash_code;
  } else {
    const auto path = RealOrFullPath();
    entry->hash_code = kHasher(path);
    return entry->hash_code;
  }
}

FileManagerImpl::FileManagerImpl(PathKind path_kind_)
    : path_buff(),
      root(new FileEntry(this)),
      path_kind(path_kind_),
      path_separator(path_kind == PathKind::kWindows ? '\\' : '/') {

  if (PathKind::kPosix == path_kind) {
    root->full_path = "/";
  }

  root->real_path_entry = root.get();
  root->parent = root.get();

  // Pre-allocate a decent amount of space for path parts.
  path_parts.resize(64);
  for (auto &part : path_parts) {
    part.resize(64);
  }

  // Compute the current working directory.
  path_buff.resize(kPathBuffSize);
  auto cwd_str = getcwd(&(path_buff[0]), kPathBuffSize - 1);
  if (!cwd_str || !cwd_str[0]) {
    perror("Couldn't get current working directory");
    abort();
  }

  path_buff.resize(strlen(path_buff.data()));

  // Have to make the first CWD manually, as all future paths depend upon it.
  size_t num_parts = SplitPathBuff();
  assert(0 < num_parts);
  path_stack.push_back(Path(GetEntryFor(root.get(), num_parts, true)));
}

// Get the current working directory.
Path FileManagerImpl::CurrentDirectory(void) const {
  return path_stack.back();
}

// Enter into the directory `path`, and push it onto the working directory
// stack. If there's an error then nothing is pushed on the stack.
std::error_code FileManagerImpl::PushDirectory(Path path) {
  if (!path.IsDirectory()) {
    return std::error_code(ENOTDIR, std::system_category());
  }
  path_stack.push_back(path);
  return std::error_code();
}

// Try to create a directory.
std::error_code FileManagerImpl::CreateDirectory(Path path) {
  const auto &path_str = path.RealOrFullPath();
  auto ret = mkdir(path_str.data(), 0666);
  auto err = errno;
  if (-1 == ret) {
    if (err != EEXIST) {
      return std::error_code(err, std::system_category());
    }
  }
  return std::error_code();
}

// Pops the top directory off the stack, and changes the current working
// directory to the next one down off the stack. If the latter operation
// fails then the program will abort.
void FileManagerImpl::PopDirectory(void) {
  path_stack.pop_back();
  assert(!path_stack.empty());
}

#ifndef ENOTDIR
#  define ENOTDIR ENOENT
#endif

size_t FileManagerImpl::SplitPathBuff(void) {
  size_t num_parts = 0;

  for (auto &part : path_parts) {
    part.clear();
  }

  auto buff = &(path_buff[0]);
  const auto buff_end = &(path_buff.back());

  while (buff <= buff_end && buff[0]) {
    if (buff[0] == path_separator) {
      buff[0] = '\0';
      buff++;
      continue;
    }

    auto next_separator = strchr(buff, path_separator);
    size_t name_len = 0;
    if (next_separator) {
      name_len = static_cast<size_t>(next_separator - buff);
    } else {
      name_len = strlen(buff);
    }

    assert(0 < name_len);

    buff[name_len] = '\0';
    auto name = buff;

    if (path_parts.size() <= num_parts) {
      path_parts.emplace_back(buff);

    } else {
      auto &part = path_parts[num_parts];
      part.insert(part.begin(), name, &(name[name_len]));
    }

    buff += name_len + 1;
    num_parts++;
  }
  return num_parts;
}

FileEntry *FileManagerImpl::GetEntryFor(FileEntry *dir, size_t num_parts,
                                        bool is_absolute) {
  if (!num_parts) {
    return dir;
  }

  size_t i = 0U;

  if (path_kind == PathKind::kWindows) {

    // Figure out if it looks like an absolute path. We use a path part
    // ending with a colon to signal that it's a drive.
    if (path_parts[0].back() == ':') {
      is_absolute = true;
      dir = root.get();

    // We think this should be an absolute path, so check that the first path
    // part ends with a colon.
    } else if (is_absolute && path_parts[0].back() != ':') {
      assert(
          false &&
          "First path part in absolute path doesn't look like a Windows drive name");
    }

    // Make sure the parent of a drive is itself.
    if (is_absolute) {
      assert(dir == root.get());
      dir = dir->GetOrAddChild(path_parts[0]);
      dir->full_path = dir->name;  // Don't let there be a separator.
      dir->parent = dir;  // Re-parent the drive.
      i++;
    }
  }

  // NOTE(pag): `.` and `..` are handled in `GetOrAddChild`.
  for (; i < num_parts; ++i) {
    dir = dir->GetOrAddChild(path_parts[i]);
  }

  return dir;
}

FileEntry *FileManagerImpl::GetEntryFor(FileEntry *dir, std::string_view path) {
  path_buff = path;
  bool is_absolute = false;

  if (path_kind == PathKind::kWindows) {
    std::replace(&(path_buff[0]), &(path_buff[path_buff.size()]), '/', '\\');
    assert(path_buff[0] != '/');
  } else {
    if (path_buff[0] == '/') {
      is_absolute = true;
      dir = root.get();
    }
  }

  size_t num_parts = SplitPathBuff();
  return GetEntryFor(dir, num_parts, is_absolute);
}

// Splits `path` incrementally and walks it until it can find an entry for
// the file.
FileEntry *FileManagerImpl::GetEntryFor(std::string_view path) {
  return GetEntryFor(CurrentDirectory().entry, path);
}

FileManager::~FileManager(void) {}

FileManager::FileManager(PathKind path_kind)
    : impl(std::make_shared<FileManagerImpl>(path_kind)) {}

FileManager::FileManager(const FileManager &that) : impl(that.impl) {}

FileManager::FileManager(FileManager &&that) noexcept
    : impl(std::move(that.impl)) {}

FileManager &FileManager::operator=(const FileManager &that) {
  impl = that.impl;
  return *this;
}

FileManager &FileManager::operator=(FileManager &&that) noexcept {
  impl = std::move(that.impl);
  return *this;
}

// Get the current working directory.
Path FileManager::CurrentDirectory(void) const {
  return impl->CurrentDirectory();
}

// Enter into the directory `path`, and push it onto the working directory
// stack. If there's an error then nothing is pushed on the stack.
std::error_code FileManager::PushDirectory(Path path) const {
  return impl->PushDirectory(path);
}

// Pops the top directory off the stack, and changes the current working
// directory to the next one down off the stack. If the latter operation
// fails then the program will abort.
void FileManager::PopDirectory(void) const {
  impl->PopDirectory();
}

// Apply `cb` to every path in the directory `dir`. Returns an error code if
// `dir` is not a directory. If `cb` returns `false` then the iteration is
// stopped.
std::error_code FileManager::ForEachPathInDirectory(
    Path dir, std::function<bool(Path)> cb) {
  auto ec = dir.ComputeRealPath();
  if (ec) {
    return ec;
  }

  auto real_path = dir.entry->real_path_entry->full_path.c_str();

#ifdef WIN32
#  warning "Implement FileManager::ForEachPathInDirectory"
  ec = std::make_error_code(std::errc::not_supported);
#else
  errno = 0;
  auto dp = opendir(real_path);
  auto err = errno;
  if (dp) {

    std::string file_name;
#  ifdef NAME_MAX
    file_name.reserve(NAME_MAX);
#  endif

    while (true) {
      errno = 0;
      auto dep = readdir(dp);
      err = errno;
      if (!dep) {
        break;
      } else {
        file_name.resize(0);
        const auto d_namlen = strlen(dep->d_name);
        file_name.insert(file_name.end(), dep->d_name,
                         &(dep->d_name[d_namlen]));
        (void) dir.entry->GetOrAddChild(file_name);
      }
    }
    closedir(dp);
  }

  ec = std::error_code(err, std::system_category());
#endif

  for (const auto &child : dir.entry->children) {
    Path child_path(child.second.get());
    if (!cb(child_path)) {
      break;
    }
  }

  return ec;
}

// Create a directory.
std::error_code FileManager::CreateDirectory(Path path) {
  return path.entry->fs->CreateDirectory(path);
}

// Remove a file.
std::error_code FileManager::RemoveFile(Path path) {
  auto ec = path.ComputeRealPath();
  if (ec) {
    return ec;
  }

  auto real_path = path.entry->real_path_entry->full_path.c_str();

#ifdef WIN32
#  warning "Implement FileManager::RemoveFile"
  ec = std::make_error_code(std::errc::not_supported);
#else
  errno = 0;
  (void) unlink(real_path);
  ec = std::error_code(errno, std::system_category());
  if (!ec) {
    if (path.entry->real_path_entry) {
      path.entry->real_path_entry->real_path_entry = nullptr;
      path.entry->real_path_entry->real_path_ec = ec;
    }
    path.entry->real_path_entry = nullptr;
    path.entry->real_path_ec = ec;
  }
#endif
  return ec;
}


}  // namespace hyde
