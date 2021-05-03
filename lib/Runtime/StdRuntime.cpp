
#include <drlojekyll/Runtime/StdRuntime.h>

namespace hyde {
namespace rt {

const StdSerialBuffer kEmptyIndexBackingBuffer{};

void BufferedWriter::WriteF64(double d) {
  WriteU64(reinterpret_cast<const uint64_t &>(d));
}

void BufferedWriter::WriteF32(float d) {
  WriteU32(reinterpret_cast<const uint32_t &>(d));
}

void BufferedWriter::WriteU64(uint64_t d) {
  key_storage.push_back(static_cast<uint8_t>(d));
  key_storage.push_back(static_cast<uint8_t>(d >> 8));
  key_storage.push_back(static_cast<uint8_t>(d >> 16));
  key_storage.push_back(static_cast<uint8_t>(d >> 24));

  key_storage.push_back(static_cast<uint8_t>(d >> 32));
  key_storage.push_back(static_cast<uint8_t>(d >> 40));
  key_storage.push_back(static_cast<uint8_t>(d >> 48));
  key_storage.push_back(static_cast<uint8_t>(d >> 56));
}

void BufferedWriter::WriteU32(uint32_t d) {
  key_storage.push_back(static_cast<uint8_t>(d >> 24));
  key_storage.push_back(static_cast<uint8_t>(d >> 16));
  key_storage.push_back(static_cast<uint8_t>(d >> 8));
  key_storage.push_back(static_cast<uint8_t>(d));
}

void BufferedWriter::WriteU16(uint16_t h) {
  key_storage.push_back(static_cast<uint8_t>(h >> 8));
  key_storage.push_back(static_cast<uint8_t>(h));
}

void BufferedWriter::WriteU8(uint8_t b) {
  key_storage.push_back(b);
}

// Get the state of the specified columns (key)
TupleState TableImpl::GetState(void) const {
  auto it = backing_store.find(key_data);
  if (it != backing_store.end()) {
    return it->second;
  } else {
    return TupleState::kAbsent;
  }
}

bool TableImpl::KeyExists(void) const {
  return backing_store.find(key_data) != backing_store.end();
}

std::pair<bool, bool>
TableImpl::TryChangeStateFromAbsentOrUnknownToPresent(void) {
  auto [it, added] = backing_store.emplace(key_data, TupleState::kPresent);
  if (!added) {
    auto &state = it->second;
    if (state == TupleState::kAbsent || state == TupleState::kUnknown) {
      state = TupleState::kPresent;
      return {true, false};
    } else {
      return {false, false};
    }
  } else {
    all_key_data.insert(all_key_data.end(), key_data.begin(), key_data.end());
    return {true, true};
  }
}

std::pair<bool, bool> TableImpl::TryChangeStateFromAbsentToPresent(void) {
  auto [it, added] = backing_store.emplace(key_data, TupleState::kPresent);
  if (!added) {
    auto &state = it->second;
    if (state == TupleState::kAbsent) {
      state = TupleState::kPresent;
      return {true, false};
    } else {
      return {false, false};
    }
  } else {
    all_key_data.insert(all_key_data.end(), key_data.begin(), key_data.end());
    return {true, true};
  }
}

bool TableImpl::TryChangeStateFromPresentToUnknown(void) {
  auto it = backing_store.find(key_data);
  if (it == backing_store.end() || it->second != TupleState::kPresent) {
    return false;
  } else {
    it->second = TupleState::kUnknown;
    return true;
  }
}

bool TableImpl::TryChangeStateFromUnknownToAbsent(void) {
  auto it = backing_store.find(key_data);
  if (it == backing_store.end() || it->second != TupleState::kUnknown) {
    return false;
  } else {
    it->second = TupleState::kAbsent;
    return true;
  }
}

}  // namespace rt
}  // namespace hyde
