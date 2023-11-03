#include<iostream>

class Class1 {
    public:
        Class1() : func1(this), func2(this), total_functor_calls(0) {}

        void start_recursion() {
            func1(0);
        }

        int get_total_func_call() { return total_functor_calls; };
    
    private:

        struct Functor1 {            
            Class1* ptr_c;

            Functor1(Class1* _ptr_c) {
                ptr_c = _ptr_c;
            }

            void operator() (int x) {
                std::cout << "func1() called with x = " << x << std::endl;
                ptr_c->total_functor_calls++;
                if (x >= 10) {
                    std::cout << "func1: Last value of x: " << x << std::endl;
                } else {
                    x += 2;
                    ptr_c->func2(x);
                }
            }

        };

        struct Functor2 {
            Class1* ptr_c;

            Functor2(Class1* _ptr_c) {
                ptr_c = _ptr_c;
            }

            void operator() (int x) {
                ptr_c->total_functor_calls++;
                std::cout << "func2() called with x = " << x << std::endl;
                if (x >= 10) {
                    std::cout << "func2: Last value of x: " << x << std::endl;
                } else {
                    x -= 1;
                    ptr_c->func1(x);
                }
            }
        };

        Functor1 func1;
        Functor2 func2;
        int total_functor_calls;
};

struct Functor4;
void Functor4::operator()(int x);

struct Functor3 {
    void operator() (int x) {
        // ptr_c->total_functor_calls++;
        // std::cout << "func2() called with x = " << x << std::endl;
        if (x >= 10) {
            std::cout << "func4: Last value of x: " << x << std::endl;
        } else {
            x -= 1;
            Functor4()(x);
        }
    }
};

struct Functor4 {
    void operator() (int x) {
        // ptr_c->total_functor_calls++;
        // std::cout << "func2() called with x = " << x << std::endl;
        if (x >= 10) {
            std::cout << "func4: Last value of x: " << x << std::endl;
        } else {
            x += 2;
            Functor3(x);
        }
    }
};

int main() {
    // Class1 c1;
    // std::cout << "Starting Test" << std::endl;
    // c1.start_recursion();
    // int f = c1.get_total_func_call();
    // std::cout << "Total functor calls: " << f << std::endl;
    // return 0;

    Functor4()(0);

}