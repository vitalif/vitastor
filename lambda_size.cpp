#include <iostream>
#include <functional>
#include <array>
#include <cstdlib> // for malloc() and free()
using namespace std;

// replace operator new and delete to log allocations
void* operator new(std::size_t n)
{
    cout << "Allocating " << n << " bytes" << endl;
    return malloc(n);
}

void operator delete(void* p) throw()
{
    free(p);
}

class test
{
public:
    std::string s;
    void a(std::function<void()> & f, const char *str)
    {
        auto l = [this, str]() { cout << str << " ? " << s << " from this\n"; };
        cout << "Assigning lambda3 of size " << sizeof(l) << endl;
        f = l;
    }
};

int main()
{
    std::array<char, 16> arr1;
    auto lambda1 = [arr1](){};
    cout << "Assigning lambda1 of size " << sizeof(lambda1) << endl;
    std::function<void()> f1 = lambda1;

    std::array<char, 17> arr2;
    auto lambda2 = [arr2](){};
    cout << "Assigning lambda2 of size " << sizeof(lambda2) << endl;
    std::function<void()> f2 = lambda2;

    test t;
    std::function<void()> f3;
    t.s = "str";
    t.a(f3, "huyambda");
    f3();
}
