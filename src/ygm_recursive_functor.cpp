#include <ygm/comm.hpp> 
#include <ygm/collective.hpp>
#include <ygm/container/array.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <iostream>

class TestClass {
    using self_ygm_ptr_type  = typename ygm::ygm_ptr<TestClass>;

    public:
        TestClass(ygm::comm &comm, int id) : m_comm(comm),
                                             m_array(comm, 11), 
                                             m_id(id), 
                                             pthis(this),
                                             func1(pthis),
                                             func2(pthis),
                                             total_functor_calls(0) {
            // Functor1 func1(pthis);
            // Functor2 func2(pthis);
            // func2(pthis);
        }

        void start_recursion() {
            func1(0);
        }

        int get_total_func_call() { 
            return ygm::sum(total_functor_calls, m_comm);            
        };
    
    // private:

        struct Functor1 {            
            // TestClass* ptr_c;

            self_ygm_ptr_type  ptr_c;

            // Functor1() {
            // }

            Functor1(self_ygm_ptr_type _ptr_c) {
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
                    if (ptr_c->m_id == 1) {
                        ptr_c->m_array.async_set(x, 1);
                        // ptr_c->m_array.async_visit(x, ptr_c->func3, 42); // Does not work 
                        // ptr_c->m_array.async_visit(x, [](uint32_t idx, uint32_t val){
                        //     std::cout << "Visiting idx: " << idx << std::endl;
                        // });
                        // ptr_c->m_array.async_visit(x, [](uint32_t idx, uint32_t val, uint32_t t){
                        //     std::cout << "Visiting idx: " << idx << " with optional argument: " << t << std::endl;
                        // }, 42);
                    }
                }
            }

        };

        struct Functor2 {
            // TestClass* ptr_c;
            
            self_ygm_ptr_type  ptr_c;

            // Functor2() {
            // }

            Functor2(self_ygm_ptr_type _ptr_c) {
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
                    if (ptr_c->m_id == 1) {
                        ptr_c->m_array.async_set(x, 1);
                        // ptr_c->m_array.async_visit(x, ptr_c->func3, 42); // Does not work
                        // ptr_c->m_array.async_visit(x, [](uint32_t idx, uint32_t val){
                        //     std::cout << "Visiting idx: " << idx << std::endl;
                        // });
                        // ptr_c->m_array.async_visit(x, [](uint32_t idx, uint32_t val, uint32_t t){
                        //     std::cout << "Visiting idx: " << idx << " with optional argument: " << t << std::endl;
                        // }, 42);
                    }
                }
            }
        };

        // struct Functor3 {
        //     TestClass* ptr_c;

        //     Functor3(TestClass* _ptr_c) {
        //         ptr_c = _ptr_c;
        //     }

        //     void operator() (uint32_t idx, uint32_t val, uint32_t t) {
        //         ptr_c->m_comm.cout() << "Visiting idx: " << idx << " using functor 3" << std::endl;
        //     }
        // };

        ygm::comm &m_comm;
        ygm::container::array<uint32_t> m_array;
        int m_id;
        Functor1 func1;
        Functor2 func2;
        self_ygm_ptr_type pthis;
        // Functor3 func3;
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

    c1.m_array.for_all([&world](uint32_t idx, uint32_t val){
        world.cout("c1 - Idx: ", idx, " Val: ", val);
    });

    c2.m_array.for_all([&world](uint32_t idx, uint32_t val){
        world.cout("c2 - Idx: ", idx, " Val: ", val);
    });
    return 0;
}