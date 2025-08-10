// Copied from https://github.com/martinus/unordered_dense, version 4.5.0
// Licensed under the MIT License <http://opensource.org/licenses/MIT>.
// SPDX-License-Identifier: MIT
// Copyright (c) 2022-2024 Martin Leitner-Ankerl <martin.ankerl@gmail.com>

#pragma once

#include <cstdint>          // for uint64_t, uint32_t, uint8_t, UINT64_C
#include <cstring>          // for size_t, memcpy, memset
#include <functional>       // for equal_to, hash
#include <memory>           // for allocator, allocator_traits, shared_ptr

#if defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__clang__)
#    define ANKERL_UNORDERED_DENSE_LIKELY(x) __builtin_expect(x, 1)   // NOLINT(cppcoreguidelines-macro-usage)
#    define ANKERL_UNORDERED_DENSE_UNLIKELY(x) __builtin_expect(x, 0) // NOLINT(cppcoreguidelines-macro-usage)
#else
#    define ANKERL_UNORDERED_DENSE_LIKELY(x) (x)   // NOLINT(cppcoreguidelines-macro-usage)
#    define ANKERL_UNORDERED_DENSE_UNLIKELY(x) (x) // NOLINT(cppcoreguidelines-macro-usage)
#endif

// This is a stripped-down implementation of wyhash: https://github.com/wangyi-fudan/wyhash
// No big-endian support (because different values on different machines don't matter),
// hardcodes seed and the secret, reformats the code, and clang-tidy fixes.
namespace wyhash {

namespace detail {

inline void mum(uint64_t* a, uint64_t* b) {
#    if defined(__SIZEOF_INT128__)
    __uint128_t r = *a;
    r *= *b;
    *a = static_cast<uint64_t>(r);
    *b = static_cast<uint64_t>(r >> 64U);
#    elif defined(_MSC_VER) && defined(_M_X64)
    *a = _umul128(*a, *b, b);
#    else
    uint64_t ha = *a >> 32U;
    uint64_t hb = *b >> 32U;
    uint64_t la = static_cast<uint32_t>(*a);
    uint64_t lb = static_cast<uint32_t>(*b);
    uint64_t hi{};
    uint64_t lo{};
    uint64_t rh = ha * hb;
    uint64_t rm0 = ha * lb;
    uint64_t rm1 = hb * la;
    uint64_t rl = la * lb;
    uint64_t t = rl + (rm0 << 32U);
    auto c = static_cast<uint64_t>(t < rl);
    lo = t + (rm1 << 32U);
    c += static_cast<uint64_t>(lo < t);
    hi = rh + (rm0 >> 32U) + (rm1 >> 32U) + c;
    *a = lo;
    *b = hi;
#    endif
}

// multiply and xor mix function, aka MUM
inline auto mix(uint64_t a, uint64_t b) -> uint64_t {
    mum(&a, &b);
    return a ^ b;
}

// read functions. WARNING: we don't care about endianness, so results are different on big endian!
inline auto r8(const uint8_t* p) -> uint64_t {
    uint64_t v{};
    std::memcpy(&v, p, 8U);
    return v;
}

inline auto r4(const uint8_t* p) -> uint64_t {
    uint32_t v{};
    std::memcpy(&v, p, 4);
    return v;
}

// reads 1, 2, or 3 bytes
inline auto r3(const uint8_t* p, size_t k) -> uint64_t {
    return (static_cast<uint64_t>(p[0]) << 16U) | (static_cast<uint64_t>(p[k >> 1U]) << 8U) | p[k - 1];
}

inline auto hash(void const* key, size_t len) -> uint64_t {
    static uint64_t secret[4] = {UINT64_C(0xa0761d6478bd642f),
        UINT64_C(0xe7037ed1a0b428db),
        UINT64_C(0x8ebc6af09c88c6e3),
        UINT64_C(0x589965cc75374cc3)};

    auto const* p = static_cast<uint8_t const*>(key);
    uint64_t seed = secret[0];
    uint64_t a{};
    uint64_t b{};
    if (ANKERL_UNORDERED_DENSE_LIKELY(len <= 16)) {
        if (ANKERL_UNORDERED_DENSE_LIKELY(len >= 4)) {
            a = (r4(p) << 32U) | r4(p + ((len >> 3U) << 2U));
            b = (r4(p + len - 4) << 32U) | r4(p + len - 4 - ((len >> 3U) << 2U));
        } else if (ANKERL_UNORDERED_DENSE_LIKELY(len > 0)) {
            a = r3(p, len);
            b = 0;
        } else {
            a = 0;
            b = 0;
        }
    } else {
        size_t i = len;
        if (ANKERL_UNORDERED_DENSE_UNLIKELY(i > 48)) {
            uint64_t see1 = seed;
            uint64_t see2 = seed;
            do {
                seed = mix(r8(p) ^ secret[1], r8(p + 8) ^ seed);
                see1 = mix(r8(p + 16) ^ secret[2], r8(p + 24) ^ see1);
                see2 = mix(r8(p + 32) ^ secret[3], r8(p + 40) ^ see2);
                p += 48;
                i -= 48;
            } while (ANKERL_UNORDERED_DENSE_LIKELY(i > 48));
            seed ^= see1 ^ see2;
        }
        while (ANKERL_UNORDERED_DENSE_UNLIKELY(i > 16)) {
            seed = mix(r8(p) ^ secret[1], r8(p + 8) ^ seed);
            i -= 16;
            p += 16;
        }
        a = r8(p + i - 16);
        b = r8(p + i - 8);
    }

    return mix(secret[1] ^ len, mix(a ^ secret[1], b ^ seed));
}

inline auto hash(uint64_t x) -> uint64_t {
    return mix(x, UINT64_C(0x9E3779B97F4A7C15));
}

} // namespace detail

template <typename T, typename Enable = void>
struct hash {
    auto operator()(T const& obj) const noexcept(noexcept(std::declval<std::hash<T>>().operator()(std::declval<T const&>())))
        -> uint64_t {
        return std::hash<T>{}(obj);
    }
};

template <typename T>
struct hash<T, typename std::hash<T>::is_avalanching> {
    using is_avalanching = void;
    auto operator()(T const& obj) const noexcept(noexcept(std::declval<std::hash<T>>().operator()(std::declval<T const&>())))
        -> uint64_t {
        return std::hash<T>{}(obj);
    }
};

template <typename CharT>
struct hash<std::basic_string<CharT>> {
    using is_avalanching = void;
    auto operator()(std::basic_string<CharT> const& str) const noexcept -> uint64_t {
        return detail::hash(str.data(), sizeof(CharT) * str.size());
    }
};

template <class T>
struct hash<T*> {
    using is_avalanching = void;
    auto operator()(T* ptr) const noexcept -> uint64_t {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return detail::hash(reinterpret_cast<uintptr_t>(ptr));
    }
};

template <class T>
struct hash<std::unique_ptr<T>> {
    using is_avalanching = void;
    auto operator()(std::unique_ptr<T> const& ptr) const noexcept -> uint64_t {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return detail::hash(reinterpret_cast<uintptr_t>(ptr.get()));
    }
};

template <class T>
struct hash<std::shared_ptr<T>> {
    using is_avalanching = void;
    auto operator()(std::shared_ptr<T> const& ptr) const noexcept -> uint64_t {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return detail::hash(reinterpret_cast<uintptr_t>(ptr.get()));
    }
};

template <typename Enum>
struct hash<Enum, typename std::enable_if<std::is_enum<Enum>::value>::type> {
    using is_avalanching = void;
    auto operator()(Enum e) const noexcept -> uint64_t {
        using underlying = typename std::underlying_type_t<Enum>;
        return detail::hash(static_cast<underlying>(e));
    }
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#    define ANKERL_UNORDERED_DENSE_HASH_STATICCAST(T)                    \
        template <>                                                      \
        struct hash<T> {                                                 \
            using is_avalanching = void;                                 \
            auto operator()(T const& obj) const noexcept -> uint64_t {   \
                return detail::hash(static_cast<uint64_t>(obj)); \
            }                                                            \
        }

#    if defined(__GNUC__) && !defined(__clang__)
#        pragma GCC diagnostic push
#        pragma GCC diagnostic ignored "-Wuseless-cast"
#    endif
// see https://en.cppreference.com/w/cpp/utility/hash
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(bool);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(char);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(signed char);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(unsigned char);
#    if ANKERL_UNORDERED_DENSE_CPP_VERSION >= 202002L && defined(__cpp_char8_t)
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(char8_t);
#    endif
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(char16_t);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(char32_t);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(wchar_t);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(short);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(unsigned short);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(int);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(unsigned int);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(long);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(long long);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(unsigned long);
ANKERL_UNORDERED_DENSE_HASH_STATICCAST(unsigned long long);

#    undef ANKERL_UNORDERED_DENSE_HASH_STATICCAST
#    undef ANKERL_UNORDERED_DENSE_LIKELY
#    undef ANKERL_UNORDERED_DENSE_UNLIKELY

#    if defined(__GNUC__) && !defined(__clang__)
#        pragma GCC diagnostic pop
#    endif

} // namespace wyhash
