#
# Function for setting up a target that uses YGM
#
function(setup_ygm_target exe_name)
    if (NOT MPI_CXX_FOUND)
        message(WARNING "YGM requires MPI; skip building ${exe_name}.")
        return()
    endif ()

    target_link_libraries(${exe_name} PRIVATE MPI::MPI_CXX)
    target_link_libraries(${exe_name} PRIVATE cereal)
    target_link_libraries(${exe_name} PRIVATE Threads::Threads)
    target_include_directories(${exe_name} PRIVATE ${Boost_INCLUDE_DIRS})
    if (${CMAKE_HOST_SYSTEM_NAME} MATCHES "Linux")
        target_link_libraries(${exe_name} PRIVATE rt)
    endif ()
    target_link_libraries(${exe_name} PRIVATE ygm::ygm)
    
endfunction()


function(add_kpath_executable name source)
    add_executable(${name} ${source})
    #add_common_compile_options(${name})
    setup_ygm_target(${name})
    target_include_directories(${name} PUBLIC "${CMAKE_SOURCE_DIR}/include")
endfunction()

add_kpath_executable(delegated_k_path delegated_k_path.cpp)
add_kpath_executable(delegated_k_path2 delegated_k_path2.cpp)
add_kpath_executable(delegated_k_path3 delegated_k_path3.cpp)
add_kpath_executable(random_walks random_walks.cpp)
add_kpath_executable(undelegated_k_path undelegated_k_path.cpp)
add_kpath_executable(no_rejection_sampling no_rejection_sampling.cpp)
add_kpath_executable(param_test_k_path param_test_k_path.cpp)
add_kpath_executable(ygm_recursive_functor ygm_recursive_functor.cpp)
add_kpath_executable(rant_graph_test rant_graph_test.cpp)
