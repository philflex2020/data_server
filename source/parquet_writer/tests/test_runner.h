/**
 * test_runner.h  —  minimal zero-dependency test framework
 *
 * Usage:
 *   TEST("my test") { EXPECT_EQ(1+1, 2); EXPECT_TRUE(foo()); }
 *   int main() { RUN_ALL_TESTS(); }
 */
#pragma once

#include <cmath>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

struct _TestCase { std::string name; std::function<void()> fn; };

inline std::vector<_TestCase>& _test_registry() {
    static std::vector<_TestCase> v;
    return v;
}

struct _TestReg {
    _TestReg(const char* name, std::function<void()> fn) {
        _test_registry().push_back({name, fn});
    }
};

// Double-indirection forces __LINE__ to expand before token-paste
#define _T_CAT(a, b) a##b
#define _T_CAT2(a, b) _T_CAT(a, b)

#define TEST(name) \
    static void _T_CAT2(_tf_, __LINE__)(); \
    static _TestReg _T_CAT2(_tr_, __LINE__)(name, _T_CAT2(_tf_, __LINE__)); \
    static void _T_CAT2(_tf_, __LINE__)()

// ---- assertion macros ----

#define EXPECT_TRUE(expr) do { \
    if (!(expr)) { \
        std::ostringstream _s; \
        _s << "    EXPECT_TRUE(" #expr ") failed  at " __FILE__ ":" << __LINE__; \
        throw std::runtime_error(_s.str()); \
    } \
} while(0)

#define EXPECT_FALSE(expr)  EXPECT_TRUE(!(expr))

#define EXPECT_EQ(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (!(_a == _b)) { \
        std::ostringstream _s; \
        _s << "    EXPECT_EQ(" #a ", " #b ") failed\n" \
           << "      got:      " << _a << "\n" \
           << "      expected: " << _b << "\n" \
           << "      at " __FILE__ ":" << __LINE__; \
        throw std::runtime_error(_s.str()); \
    } \
} while(0)

#define EXPECT_NE(a, b) do { \
    if ((a) == (b)) { \
        std::ostringstream _s; \
        _s << "    EXPECT_NE(" #a ", " #b ") failed (both == " << (a) << ")" \
           << "  at " __FILE__ ":" << __LINE__; \
        throw std::runtime_error(_s.str()); \
    } \
} while(0)

#define EXPECT_NEAR(a, b, eps) do { \
    double _nr_a = (a); double _nr_b = (b); \
    if (std::fabs(_nr_a - _nr_b) > (eps)) { \
        std::ostringstream _s; \
        _s << "    EXPECT_NEAR(" #a ", " #b ", " #eps ") failed\n" \
           << "      got:      " << _nr_a << "\n" \
           << "      expected: " << _nr_b << " ± " << (eps) << "\n" \
           << "      at " __FILE__ ":" << __LINE__; \
        throw std::runtime_error(_s.str()); \
    } \
} while(0)

// ---- runner ----

inline int _run_all_tests(const char* suite = nullptr) {
    int pass = 0, fail = 0;
    if (suite) std::cout << "\n=== " << suite << " ===\n";
    for (auto& tc : _test_registry()) {
        try {
            tc.fn();
            std::cout << "  PASS  " << tc.name << "\n";
            ++pass;
        } catch (const std::exception& e) {
            std::cout << "  FAIL  " << tc.name << "\n" << e.what() << "\n";
            ++fail;
        }
    }
    std::cout << "\n" << pass << " passed, " << fail << " failed\n";
    return fail > 0 ? 1 : 0;
}

#define RUN_ALL_TESTS(...)  return _run_all_tests(__VA_ARGS__)
