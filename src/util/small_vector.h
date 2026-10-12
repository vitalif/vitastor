// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <stddef.h>
#include <new>
#include <utility>
#include <type_traits>

// Minimal vector with small-buffer optimization: up to INLINE elements live
// inside the object itself, heap is only used beyond that
template <typename T, size_t INLINE = 4>
class small_vector_t
{
public:
    typedef T* iterator;
    typedef const T* const_iterator;

    small_vector_t() : data_ptr(inline_data()), cap(INLINE)
    {
    }

    ~small_vector_t()
    {
        clear();
        if (cap > INLINE)
            delete[] reinterpret_cast<char*>(data_ptr);
    }

    small_vector_t(const small_vector_t &other) : small_vector_t()
    {
        reserve(other.sz);
        for (size_t i = 0; i < other.sz; i++)
            new (data_ptr + i) T(other.data_ptr[i]);
        sz = other.sz;
    }

    small_vector_t(small_vector_t &&other) noexcept : small_vector_t()
    {
        if (other.cap > INLINE)
        {
            data_ptr = other.data_ptr;
            cap = other.cap;
            sz = other.sz;
            other.data_ptr = other.inline_data();
            other.sz = 0;
            other.cap = INLINE;
        }
        else
        {
            for (; sz < other.sz; sz++)
                new (data_ptr + sz) T(std::move(other.data_ptr[sz]));
            destroy_other(other);
        }
    }

    small_vector_t& operator=(const small_vector_t &other)
    {
        if (this != &other)
        {
            clear();
            reserve(other.sz);
            for (size_t i = 0; i < other.sz; i++)
                new (data_ptr + i) T(other.data_ptr[i]);
            sz = other.sz;
        }
        return *this;
    }

    small_vector_t& operator=(small_vector_t &&other) noexcept
    {
        if (this != &other)
        {
            clear();
            if (cap > INLINE)
                delete[] reinterpret_cast<char*>(data_ptr);
            if (other.cap > INLINE)
            {
                data_ptr = other.data_ptr;
                cap = other.cap;
                sz = other.sz;
                other.data_ptr = other.inline_data();
                other.sz = 0;
                other.cap = INLINE;
            }
            else
            {
                data_ptr = inline_data();
                cap = INLINE;
                for (sz = 0; sz < other.sz; sz++)
                    new (data_ptr + sz) T(std::move(other.data_ptr[sz]));
                destroy_other(other);
            }
        }
        return *this;
    }

    template <typename... Args>
    T& emplace_back(Args&&... args)
    {
        if (sz == cap)
            grow();
        return *(new (data_ptr + sz++) T(std::forward<Args>(args)...));
    }

    void push_back(const T &v) { emplace_back(v); }
    void push_back(T &&v) { emplace_back(std::move(v)); }

    void pop_back() { data_ptr[--sz].~T(); }

    T& operator[](size_t i) { return data_ptr[i]; }
    const T& operator[](size_t i) const { return data_ptr[i]; }
    T& front() { return data_ptr[0]; }
    T& back() { return data_ptr[sz-1]; }
    const T& front() const { return data_ptr[0]; }
    const T& back() const { return data_ptr[sz-1]; }

    T* data() { return data_ptr; }
    const T* data() const { return data_ptr; }
    size_t size() const { return sz; }
    size_t capacity() const { return cap; }
    bool empty() const { return sz == 0; }
    bool is_inline() const { return cap <= INLINE; }

    iterator begin() { return data_ptr; }
    iterator end() { return data_ptr + sz; }
    const_iterator begin() const { return data_ptr; }
    const_iterator end() const { return data_ptr + sz; }

    void clear()
    {
        for (size_t i = 0; i < sz; i++)
            data_ptr[i].~T();
        sz = 0;
    }

    // Grows only; inline storage is kept unless n exceeds INLINE
    void reserve(size_t n)
    {
        if (n <= cap)
            return;
        size_t new_cap = n > cap*2 ? n : cap*2;
        T *nd = reinterpret_cast<T*>(new char[sizeof(T)*new_cap]);
        for (size_t i = 0; i < sz; i++)
            new (nd + i) T(std::move(data_ptr[i]));
        for (size_t i = 0; i < sz; i++)
            data_ptr[i].~T();
        if (cap > INLINE)
            delete[] reinterpret_cast<char*>(data_ptr);
        data_ptr = nd;
        cap = new_cap;
    }

private:
    T *inline_data() { return reinterpret_cast<T*>(inline_store); }

    // Moved-from inline elements must be destroyed explicitly:
    // the source stays alive with sz = 0 and its destructor won't reach them
    void destroy_other(small_vector_t &other)
    {
        for (size_t i = 0; i < other.sz; i++)
            other.data_ptr[i].~T();
        other.sz = 0;
    }

    void grow()
    {
        reserve(cap*2 > INLINE ? cap*2 : INLINE+1);
    }

    T *data_ptr;
    size_t sz = 0;
    size_t cap;
    alignas(T) char inline_store[INLINE * sizeof(T)];
};
