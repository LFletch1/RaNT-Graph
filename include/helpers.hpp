// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/comm.hpp>

struct Params {
    uint32_t k; // Take paths of length at most k
    uint64_t p; // Number of paths to take
    uint32_t d; // Degree threshold to determine whether to delegate a vertex
    uint32_t r; // Rejection sampling threshold to determine whether to rejection sample
    uint32_t s; // Seed for randomization initialization
    uint32_t v; // Scale for number of vertices in RMAT generated graph, i.e. 2^v
    uint64_t e; // Global edge count in RMAT generated graph
    std::string f; // file or directory of json data to create graph
    char m;
};

Params parse_args(int argc, char *argv[], ygm::comm &comm) {
    // Defaults
    uint32_t k = 10;
    uint64_t p = 1000; 
    uint32_t d = comm.size();
    uint32_t r = 2;
    uint32_t s = 42;
    uint32_t v = 11;
    uint64_t e = 10000;
    std::string f = "";
    char m = '\0';
    Params params{k, p, d, r, s, v, e, f, m};
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            char flag = argv[i][1];
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                uint32_t value;
                uint64_t lvalue;
                if (flag == 'p' || flag == 'e') {
                  lvalue = std::atol(argv[i + 1]);
                } 
                else if (flag != 'f' && flag != 'm') {
                  value = std::atoi(argv[i + 1]);
                } 
                switch (flag) {
                    case 'k':
                        params.k = value;
                        break;
                    case 'p':
                        params.p = lvalue;
                        break;
                    case 'd':
                        params.d = value;
                        break;
                    case 'r':
                        params.r = value;
                        break;
                    case 's':
                        params.s = value;
                        break;
                    case 'v':
                        params.v = value;
                        break;
                    case 'e':
                        params.e = lvalue;
                        break;
                    case 'f':
                        params.f = argv[i + 1];
                        break;
                    case 'm': {
                        // params.m = (char) argv[i+1].erase(std::remove(argv[i+1].begin(), argv[i+1].end(), '\"'), argv[i+1].end()); 
                        std::string tmp = argv[i+1];
                        if (tmp == "t") {
                            params.m = '\t';
                        } else if (tmp == "s") {
                            params.m = ' ';
                        } 
                        else {
                            tmp.erase(std::remove(tmp.begin(), tmp.end(), '\"'), tmp.end());
                            params.m = tmp[0];
                        }
                        // params.m.erase(std::remove(params.m.begin(), params.m.end(), '\"'), params.m.end());
                        break;
                    }
                    default:
                        comm.cout0() << "Unknown flag: " << flag << std::endl;
                        break;
                }
                i++;
            } else {
                comm.cout0() << "Flag " << flag << " requires a value." << std::endl;
            }
        }
    }
    if (params.f != "") {
      ASSERT_RELEASE(params.m != '\0');
    }
    return params;
}


template<typename Iter, typename RNG>
auto select_randomly_from_vec(Iter start, Iter end, RNG &rng) {
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(rng));
    return *start;
}