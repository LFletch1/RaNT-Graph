#include <ygm/comm.hpp>
#include <ygm/collective.hpp>
#include <iostream>

class TestClass {
    public:
        TestClass(ygm::comm &comm, int id) : m_comm(comm), m_id(id), func1(this), func2(this), total_functor_calls(0) {}

        void start_recursion() {
            func1(0);
        }

        int get_total_func_call() { 
            return ygm::sum(total_functor_calls, m_comm);            
        };
    
    private:

        struct Functor1 {            
            TestClass* ptr_c;

            Functor1(TestClass* _ptr_c) {
                ptr_c = _ptr_c;
            }

            void operator() (int x) {
                std::cout << "func1() called with x = " << x << " on rank " << ptr_c->m_comm.rank() << " using the class c" << ptr_c->m_id << std::endl;
                ptr_c->total_functor_calls++;
                if (x >= 10) {
                    std::cout << "func1: Last value of x: " << x << std::endl;
                } else {
                    x += 2;
                    ptr_c->m_comm.async(1, ptr_c->func2, x);
                }
            }

        };

        struct Functor2 {
            TestClass* ptr_c;

            Functor2(TestClass* _ptr_c) {
                ptr_c = _ptr_c;
            }

            void operator() (int x) {
                ptr_c->total_functor_calls++;
                std::cout << "func2() called with x = " << x << " on rank " << ptr_c->m_comm.rank() << " using the class c" << ptr_c->m_id << std::endl;
                if (x >= 10) {
                    std::cout << "func2: Last value of x: " << x << std::endl;
                } else {
                    x -= 1;
                    ptr_c->m_comm.async(0, ptr_c->func1, x);
                }
            }
        };

        ygm::comm &m_comm;
        int m_id;
        Functor1 func1;
        Functor2 func2;
        int total_functor_calls;
};


int main(int argc, char** argv) {
    ygm::comm world(&argc, &argv);
    TestClass c1(world, 1);
    TestClass c2(world, 2);
    world.cout0() << "Starting Test" << std::endl;
    if (world.rank0()) {
        c1.start_recursion();
        c2.start_recursion();
    }
    world.barrier(); 
    int f1 = c1.get_total_func_call();
    int f2 = c2.get_total_func_call();
    world.cout0() << "Total functor calls in class c1: " << f1 << std::endl;
    world.cout0() << "Total functor calls in class c2: " << f2 << std::endl;
    return 0;
}