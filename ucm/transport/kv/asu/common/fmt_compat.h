#pragma once

#include <type_traits>

namespace fmt {
#if !defined(FMT_HAS_UNDERLYING)
template <typename E>
constexpr auto underlying(E e) noexcept -> std::underlying_type_t<E> {
    return static_cast<std::underlying_type_t<E>>(e);
}
#define FMT_HAS_UNDERLYING 1
#endif
}  // namespace fmt
