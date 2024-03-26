#include <ygm/collective.hpp> 
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/map.hpp>
#include <ygm/random.hpp>
#include <random>
#include <tuple>
#include <helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <unordered_map>
#include <ygm/detail/ygm_ptr.hpp>
#include <multi_alias_table.hpp>
#include <type_traits>
#include <mpi.h>

#pragma once

template <typename vertex_t, typename weight_t = void>
class RaNT_Graph {

  using RNG                = ygm::default_random_engine<>;
  using self_type          = RaNT_Graph<vertex_t, weight_t>;
  using self_ygm_ptr_type  = ygm::ygm_ptr<self_type>;

  private:

    using alias_table_item = std::tuple<double, vertex_t, vertex_t>;
    using Edge = typename std::conditional<std::is_void<weight_t>::value, std::pair<vertex_t,vertex_t>, std::tuple<vertex_t,vertex_t,double> >::type;



    ygm::comm&                                            m_comm;
    self_ygm_ptr_type                                     pthis;
    RNG&                                                  m_rng;
    bool                                                  m_directed;   
    bool                                                  m_weighted;   
    std::vector<Edge>                                     m_local_edges;
    uint32_t                                              m_delegation_threshold;
    uint32_t                                              m_rejection_threshold;  // Parameter for path sampling
    std::unordered_map<vertex_t, vertex_t>                m_delegated_vertex_degrees;
    std::vector<vertex_t>                                 m_local_vertices;
    uint64_t                                              m_local_paths_finished;
    uint64_t                                              m_shortcuts_taken;

    // Class variables used if graph is unweighted
    ygm::container::map<vertex_t, std::vector<vertex_t>>  m_adj_lists;
    std::unordered_map<vertex_t, std::vector<vertex_t>>   m_local_delegated_adj_lists;

    // Class variables used if graph is weighted
    using weighted_edge = std::pair<vertex_t, double>;
    ygm::container::map<vertex_t, std::vector<weighted_edge>>     m_weighted_adj_lists;
    multi_alias_table<vertex_t, vertex_t, double, RNG>            m_delegated_alias_tables;
    ygm::container::map<vertex_t, std::vector<alias_table_item>>  m_alias_tables;

  public:

    ygm::container::counting_set<vertex_t>                m_cs;

    RaNT_Graph(ygm::comm& comm, uint32_t d_thresh, RNG& rng, bool weighted = false, bool directed = false) : 
                  m_comm(comm),
                  m_adj_lists(comm),
                  m_weighted_adj_lists(comm),
                  m_alias_tables(comm),
                  m_cs(comm),
                  m_local_paths_finished(0),
                  m_shortcuts_taken(0),
                  m_rng(rng),
                  m_weighted(weighted),
                  m_directed(directed),
                  m_delegated_alias_tables(comm, rng),
                  m_delegation_threshold(d_thresh),
                  pthis(this) {}

    void clear_paths_finished() {
      m_comm.barrier();
      m_local_paths_finished = 0;
    }

    void insert_edge_locally(Edge e) {
      m_local_edges.push_back(e);
    }
    
    
    void construct_graph() {
      {
        using degree_t = vertex_t; // Degree of vertex is upperbounded by num of vertices in graph;
        std::unordered_map<vertex_t,degree_t> local_vertex_degrees;
        std::unordered_map<vertex_t,degree_t> vertex_degrees;
        auto ptr_v_degrees = m_comm.make_ygm_ptr(vertex_degrees);
        m_comm.barrier(); // Ensure ygm_ptr created on every rank
        for (Edge e : m_local_edges) {
          if (std::get<0>(e) != std::get<1>(e)) { // Not currently supporting self edges, probably could
            local_vertex_degrees[std::get<0>(e)]++; 
            if (!m_directed) {
              local_vertex_degrees[std::get<1>(e)]++;
            }
          }
        }      
        auto add_to_degree = [](vertex_t v, degree_t amount, auto ptr_vertex_degrees){
          if (ptr_vertex_degrees->find(v) != ptr_vertex_degrees->end()) {
            ptr_vertex_degrees->at(v) = ptr_vertex_degrees->at(v) + amount;
          } else {
            (*ptr_vertex_degrees)[v] = amount;
          }
        };
        for (const auto & [v, degree] : local_vertex_degrees) {
          uint32_t dest = owner(v); 
          m_comm.async(dest, add_to_degree, v, degree, ptr_v_degrees);
        }
        m_comm.barrier();
        using vrtx_deg_vec = std::vector<std::pair<vertex_t,degree_t>>;
        vrtx_deg_vec my_delegated_vertex_degrees;
        for (const auto & [v, degree] : vertex_degrees) {
          ASSERT_RELEASE(owner(v) == m_comm.rank());
          if (degree >= m_delegation_threshold) {
            my_delegated_vertex_degrees.push_back(std::make_pair(v,degree));
          }
        }
        // Determine which vertices are to be delegated and store their degrees
        auto merge_func = [](vrtx_deg_vec a, vrtx_deg_vec b){
            vrtx_deg_vec res;
            std::set_union(a.cbegin(), a.cend(), b.cbegin(), b.cend(), std::back_inserter(res));
            return res;
        };
        vrtx_deg_vec all_delegated_vertex_degrees = m_comm.all_reduce(my_delegated_vertex_degrees, merge_func); 
        m_delegated_vertex_degrees.insert(all_delegated_vertex_degrees.begin(), all_delegated_vertex_degrees.end());
      }

      m_comm.barrier();

      ASSERT_RELEASE(ygm::is_same(m_delegated_vertex_degrees.size(), m_comm));

      if (m_weighted) {

        auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };

        for (auto& edge : m_local_edges) {
          vertex_t src = std::get<0>(edge);
          vertex_t dst = std::get<1>(edge);
          double w = get_edge_weight(edge);

          if (src != dst) {
            if (m_delegated_vertex_degrees.find(src) != m_delegated_vertex_degrees.end()) {
              m_delegated_alias_tables.local_add_item(std::make_tuple(src, dst, w));
            } else {
              std::vector<weighted_edge> src_adj_list = {{dst, w}};
              m_weighted_adj_lists.async_insert_if_missing_else_visit(src, src_adj_list, add_to_adjacency_list);
            }
            if (!m_directed) {
              if (m_delegated_vertex_degrees.find(dst) != m_delegated_vertex_degrees.end()) {
                m_delegated_alias_tables.local_add_item(std::make_tuple(dst, src, w));
              } else {
                std::vector<weighted_edge> dst_adj_list = {{src, dst}};
                m_weighted_adj_lists.async_insert_if_missing_else_visit(dst, dst_adj_list, add_to_adjacency_list); 
              }
            }
          }
        }
        {
          std::vector<Edge> empty_vec;
          std::swap(empty_vec, m_local_edges); // Free the memory of m_local_edges
        }
        build_local_alias_tables();
        m_delegated_alias_tables.balance_weight();
        m_delegated_alias_tables.check_balancing();
        m_delegated_alias_tables.build_alias_tables();
        m_weighted_adj_lists.for_all([&](const auto& vertex, const auto& adj_list){
          pthis->m_local_vertices.push_back(vertex);
        });
        m_weighted_adj_lists.clear();
        for (const auto & [vertex, deg] : m_delegated_vertex_degrees) {
          if (owner(vertex) == m_comm.rank()) {
            m_local_vertices.push_back(vertex); 
          }
        }

      } else {
        // For now, use inefficient method of first distributing every edges via a 1D partitioning then having 
        // each rank distribute edges as needed. Later should probably use some sort of distribution method similar
        // to that of how the multi_alias tables are built
        // ygm::ygm_ptr local_scope_pthis = pthis;
        // m_comm.cout0("Before 1D Partitioning");
        // int64_t bytes = 0;
        // for (auto& edge : m_local_edges) {
        //   bytes += 16;
        // }
        // m_comm.cout("My bytes: ", bytes);
        

        for (auto& edge : m_local_edges) {
          vertex_t src = std::get<0>(edge);
          vertex_t dst = std::get<1>(edge);
          if (src != dst) { // Not currently supporting self edges, probably could
            auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
              adj_list.push_back(new_adj_list[0]);
            };
            std::vector<vertex_t> src_adj_list = {dst};
            m_adj_lists.async_insert_if_missing_else_visit(src, src_adj_list, add_to_adjacency_list);
            if (!m_directed) {
              std::vector<vertex_t> dst_adj_list = {src};
              m_adj_lists.async_insert_if_missing_else_visit(dst, dst_adj_list, add_to_adjacency_list); 
            }
          }
        }

        m_local_edges.clear();
        m_comm.barrier(); 
        {
          std::vector<Edge> empty_vec;
          std::swap(empty_vec, m_local_edges); // Free the memory of m_local_edges
        }

        // m_comm.cout0("After 1D partitioning");
        // bytes = 0;
        // m_adj_lists.for_all([&](const auto& v, auto& adj_list) {
        //   bytes += adj_list.size() * 4; 
        //   bytes += 4; 
        // });
        // m_comm.cout("My bytes: ", bytes);
        
        for (auto &v_degree : m_delegated_vertex_degrees) {
          vertex_t v = v_degree.first;

          auto delegate = [](const auto& v, auto& adj_list, self_ygm_ptr_type pthis) {
            ASSERT_RELEASE(adj_list.size() == pthis->m_delegated_vertex_degrees[v]);

            auto delegator = [](vertex_t v, std::vector<vertex_t> delegated_adj_list, vertex_t degree, self_ygm_ptr_type pthis) {
              pthis->m_local_delegated_adj_lists.insert({v, delegated_adj_list});
            };

            int total = 0;
            for (int r = 0; r < pthis->m_comm.size(); r++) {
              std::vector<vertex_t> part_of_adj;
              for (int i = r; i < adj_list.size(); i += pthis->m_comm.size()) {
                part_of_adj.push_back(adj_list.at(i));
              }
              total += part_of_adj.size();
              pthis->m_comm.async(r, delegator, v, part_of_adj, adj_list.size(), pthis);
            }
            ASSERT_RELEASE(total == adj_list.size());
          };

          if (owner(v) == m_comm.rank()) {
            m_adj_lists.async_visit(v, delegate, pthis);
          }
        }
        m_comm.barrier();

        // static uint32_t total_deleted;
        for (auto &v_degree : m_delegated_vertex_degrees) {
          m_adj_lists.async_erase(v_degree.first);
          // total_deleted++;
        }

        m_comm.barrier();

        m_adj_lists.for_all([&](const auto& vertex, const auto& adj_list){
          pthis->m_local_vertices.push_back(vertex);
        });

        for (const auto & [vertex, deg] : m_delegated_vertex_degrees) {
          if (owner(vertex) == m_comm.rank()) {
            m_local_vertices.push_back(vertex); 
          }
        }
        m_comm.barrier(); 
      }
    }

    void build_local_alias_tables() {
      m_weighted_adj_lists.for_all([&](const vertex_t& vertex, std::vector<weighted_edge>& adj_list){

        double total_weight = 0.0;
        for (auto& e : adj_list) {
            total_weight += e.second;
        }
        // Make average weight of items 1, so coin flip is simple across all tables 
        double avg_w8 = total_weight / adj_list.size(); 
        double new_total_weight = 0;
        for (auto& edge : adj_list) {
            edge.second /= avg_w8;
            new_total_weight += edge.second;
        }

        double new_avg_w8 = new_total_weight / adj_list.size(); // Should be 1
        if (std::abs((new_avg_w8 - 1.0)) < 0.0001) {
            ASSERT_RELEASE(std::abs((new_avg_w8 - 1.0)) < 0.0001);
        }                

        // Implementation of Vose's algorithm, utilized Keith Schwarz numerically stable version
        // https://www.keithschwarz.com/darts-dice-coins/
        std::vector<weighted_edge> heavy_edges;
        std::vector<weighted_edge> light_edges;
        for (auto& edge : adj_list) {
            if (edge.second < 1.0) {
                light_edges.push_back(edge);
            } else {
                heavy_edges.push_back(edge);
            }
        }

        auto add_to_alias_table = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };

        while (!light_edges.empty() && !heavy_edges.empty()) {
            weighted_edge l = light_edges.back();
            weighted_edge h = heavy_edges.back(); 
            std::vector<alias_table_item> ati_vec = {{l.second, l.first, h.first}};
            pthis->m_alias_tables.async_insert_if_missing_else_visit(vertex, ati_vec, add_to_alias_table);
            h.second = (h.second + l.second) - 1.0;
            light_edges.pop_back(); 
            if (h.second < 1) {
                light_edges.push_back(h);
                heavy_edges.pop_back();
            }   
        }

        // Either heavy items or light_items is empty, need to flush the non empty 
        // vector and add them to the alias table with a p value of 1
        while (!heavy_edges.empty()) {
            weighted_edge h = heavy_edges.back();
            std::vector<alias_table_item> ati_vec = {{1.0, h.first, 0}};
            pthis->m_alias_tables.async_insert_if_missing_else_visit(vertex, ati_vec, add_to_alias_table);
            heavy_edges.pop_back();
        }
        while (!light_edges.empty()) {
            weighted_edge l = light_edges.back();
            std::vector<alias_table_item> ati_vec = {{1.0, l.first, 0}};
            pthis->m_alias_tables.async_insert_if_missing_else_visit(vertex, ati_vec, add_to_alias_table);
            light_edges.pop_back();
        } 
      });
      m_comm.barrier();
    }

    /*

    void start_path(vertex_t v, uint32_t l) { 

      std::set<vertex_t> path{}; // First vertex will be added to path at beginning of called functor
      if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
        std::uniform_int_distribution<vertex_t> dis(0, m_delegated_vertex_degrees[v]-1);
        uint32_t global_idx = dis(m_rng);
        // m_comm.async(owner(global_idx), 
        //              async_delegated_path_step(), 
        //              v, 
        //              local_idx(global_idx), path, l, pthis);
      } else {
        // m_adj_lists.async_visit(v, async_path_step(), path, l, pthis);
      }
    }


    void take_n_paths(uint64_t n, uint32_t k) {
      m_comm.barrier();

      uint64_t ranks_total_paths = n / m_comm.size() + (n % m_comm.size() > m_comm.rank());
      std::uniform_int_distribution<uint32_t> dis(1, k);

      for (int p = 0; p < ranks_total_paths; p++) {
        uint32_t path_length = dis(m_rng);
        vertex_t vertex = select_randomly_from_vec(m_local_vertices.begin(), m_local_vertices.end(), m_rng); 
        start_path(vertex, path_length);  
      }

      m_comm.barrier();
    }
    */


    void start_walk(vertex_t v, uint32_t target_walk_length) {
      if (m_weighted) {
        if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
          m_delegated_alias_tables.async_sample(v, async_bias_delegated_walk_step(), 0, target_walk_length, pthis);
        } else {
          m_alias_tables.async_visit(v, async_bias_walk_step(), 0, target_walk_length, pthis);
        }
      } else {
        if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
          std::uniform_int_distribution<vertex_t> dis(0, m_delegated_vertex_degrees.at(v)-1);
          ASSERT_RELEASE(pthis->m_delegated_vertex_degrees.at(v) > 0);
          uint32_t global_idx = dis(m_rng);
          m_comm.async(owner(global_idx), 
                      async_delegated_walk_step(), 
                      v, 
                      local_idx(global_idx), 0, target_walk_length, pthis);
        } else {
          m_adj_lists.async_visit(v, async_walk_step(), 0, target_walk_length, pthis);
        }
      }
    }


    void take_n_walks(uint64_t n, uint32_t target_walk_length) {

      uint64_t ranks_total_paths = n / m_comm.size() + (n % m_comm.size() > m_comm.rank());

      for (int p = 0; p < ranks_total_paths; p++) {
        vertex_t vertex = select_randomly_from_vec(m_local_vertices.begin(), m_local_vertices.end(), m_rng); 
        start_walk(vertex, target_walk_length);  
      }

      m_comm.barrier();
    }


    uint64_t paths_finished() {
      m_comm.barrier();
      return ygm::sum(m_local_paths_finished, m_comm);
    }

    uint64_t shortcuts_taken() {
      m_comm.barrier();
      return ygm::sum(m_shortcuts_taken, m_comm);
    }

    size_t num_vertices() {
      // Correct if delegation threshold > comm.size(). If not then only will be correct for rank 0.
      m_comm.barrier();
      if (m_weighted) {
        return m_alias_tables.size() + m_delegated_vertex_degrees.size(); 
      } else {
        return m_adj_lists.size() + m_delegated_vertex_degrees.size();
      }
    }

    inline uint32_t owner(vertex_t vertex_id) {
      return (uint32_t) (vertex_id % m_comm.size());
    }

    inline uint32_t local_idx(vertex_t global_idx) {
      return global_idx / m_comm.size();
    }

    double get_edge_weight(std::pair<vertex_t,vertex_t>& edge) {
      return 1.0;
      // return void();
    }

    double get_edge_weight(std::tuple<vertex_t,vertex_t,double>& edge) {
      return std::get<2>(edge);
    }

    /*
    template<typename Container>
    void construct_from_edge_container(Container &cont) {
      // Need to check if for_all can be called on container. Do this later
      // For now assume its a bag of std::pair<vertex_t, vertex_t>
      ygm::ygm_ptr local_pthis = pthis;

      cont.for_all([&](vertex_t v1, vertex_t v2){
        if (v1 != v2) {
          std::vector<vertex_t> adj_list_1 = {v2};
          std::vector<vertex_t> adj_list_2 = {v1};
          auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
            adj_list.push_back(new_adj_list[0]);
          };
          pthis->m_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
          pthis->m_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
        }
      });

      m_comm.barrier();

      // Determine which vertices to delegate and remove duplicates from adjacency lists
      uint64_t total_edges = 0;
      std::vector<vertex_t> vertices_to_delegate;
      m_adj_lists.for_all([&](const auto& v, auto& adj_list) {
        std::sort( adj_list.begin(), adj_list.end() );
        adj_list.erase( std::unique( adj_list.begin(), adj_list.end() ), adj_list.end() );
        if (adj_list.size() >= local_pthis->m_delegation_threshold) {
          vertices_to_delegate.push_back(v);
        }
        total_edges += adj_list.size();
      });

      for (auto &v : vertices_to_delegate) {

        auto delegate = [](const auto& v, auto& adj_list, self_ygm_ptr_type pthis) {

          auto delegator = [](vertex_t v, std::vector<vertex_t> delegated_adj_list, uint64_t degree, self_ygm_ptr_type pthis) {
            pthis->m_local_delegated_adj_lists.insert({v, delegated_adj_list});
            pthis->m_delegated_vertex_degrees.insert({v, degree});
          };
          int total = 0;
          for (int r = 0; r < pthis->m_comm.size(); r++) {
            std::vector<vertex_t> part_of_adj;
            for (int i = r; i < adj_list.size(); i += pthis->m_comm.size()) {
              part_of_adj.push_back(adj_list.at(i));
            }
            total += part_of_adj.size();
            pthis->m_comm.async(r, delegator, v, part_of_adj, adj_list.size(), pthis);
          }
          ASSERT_RELEASE(total == adj_list.size());
        };

        m_adj_lists.async_visit(v, delegate, pthis);
      }
      m_comm.barrier();

      // static uint32_t total_deleted;
      for (auto &v : vertices_to_delegate) {
        m_adj_lists.async_erase(v);
        // total_deleted++;
      }

      m_comm.barrier();

      m_adj_lists.for_all([&](const auto& vertex, const auto& adj_list){
        local_pthis->m_local_vertices.push_back(vertex);
      });

      for (const auto & [key, value] : m_delegated_vertex_degrees) {
        // Should determine if vertex was originally owned by rank and only add it to local vertices
        // if it was owned by it. This prevents oversampling walks starting at delegated vertcies
        m_local_vertices.push_back(key); 
      }

      m_comm.barrier();

      // Need to redisribute edges based on whether they are delegated, can probably do this with
      // taking two passes over the adjacency list data, but will not do this for now.
    } */

    /*
    struct async_path_step {            
      
      void operator()(vertex_t vertex,
                        std::vector<vertex_t> adj_list,
                        std::set<vertex_t> path, uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        path.insert(vertex);
        pthis->m_cs.async_insert(vertex);

        // find new unvisited neighbor to visit next
        if (path.size() < l) {
          // if (adj_list.size() > pthis->m_rejection_threshold) {
          if (adj_list.size() > l) {
            bool found_unvisited = false;
            vertex_t next_vertex;
            while (!found_unvisited) {
              next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), pthis->m_rng);
              if (path.find(next_vertex) == path.end()) {
                found_unvisited = true;
              }
            }
            if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
              std::uniform_int_distribution<vertex_t> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
              uint64_t global_idx = dis(pthis->m_rng);
              pthis->m_comm.async(pthis->owner(global_idx),
                                  async_delegated_path_step(),
                                  next_vertex,
                                  pthis->local_idx(global_idx), path, l, pthis);
            } else {
              pthis->m_adj_lists.async_visit(next_vertex, async_path_step(), path, l, pthis);
            }
          } else {
            std::vector<vertex_t> unvisited;
            for (vertex_t &v : adj_list) {
              if (path.find(v) == path.end()) {
                unvisited.push_back(v);
              }
            } 
            if (unvisited.size() > 0) {
              vertex_t next_vertex = select_randomly_from_vec(unvisited.begin(), unvisited.end(), pthis->m_rng);
              if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
                std::uniform_int_distribution<uint64_t> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
                uint64_t global_idx = dis(pthis->m_rng);
                pthis->m_comm.async(pthis->owner(global_idx), 
                                    async_delegated_path_step(), 
                                    next_vertex, 
                                    pthis->local_idx(global_idx), path, l, pthis);
              } else {
                pthis->m_adj_lists.async_visit(next_vertex, async_path_step(), path, l, pthis);
              }
            } else {
              pthis->m_local_paths_finished++;
            }
          }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };
          

    struct async_delegated_path_step {            

      void operator()(uint64_t vertex,
                      uint64_t local_idx,
                      std::set<uint64_t> path,
                      uint32_t l, self_ygm_ptr_type pthis) {

        // pthis->m_comm.cout("Made it inside delegated step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        path.insert(vertex);
        if (path.size() < l) { 
          uint64_t next_vertex = pthis->m_local_delegated_adj_lists[vertex][local_idx]; 
          if (path.find(next_vertex) == path.end()) { 
            pthis->m_cs.async_insert(vertex);
            if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
              std::uniform_int_distribution<uint64_t> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
              uint64_t global_idx = dis(pthis->m_rng);

              pthis->m_comm.async(pthis->owner(global_idx),
                                  async_delegated_path_step(), 
                                  next_vertex, 
                                  pthis->local_idx(global_idx), path, l, pthis);
            } else {
              pthis->m_adj_lists.async_visit(next_vertex, async_path_step(), path, l, pthis);
            }
          } else {  // Vertex already in path so need to resample another neighbor from original vertex's
                    // the delegated adj list
            std::uniform_int_distribution<uint64_t> dis(0, pthis->m_delegated_vertex_degrees[vertex]-1);
            uint64_t global_idx = dis(pthis->m_rng);
            pthis->m_comm.async(pthis->owner(global_idx),
                                   async_delegated_path_step(),
                                   vertex,
                                   pthis->local_idx(global_idx), path, l, pthis);
          }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };
    */


    struct async_walk_step {            
      
      void operator()(vertex_t vertex,
                        std::vector<vertex_t> adj_list,
                        uint32_t walk_length, uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        // path.insert(vertex);
        // pthis->m_comm.cout("At vertex ", vertex);
        // pthis->m_cs.async_insert(vertex);
        auto& rg = *pthis;
        walk_length++;
        if (walk_length < l && adj_list.size() > 0) {
            vertex_t next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), rg.m_rng);
            // if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
            if (rg.m_delegated_vertex_degrees.find(next_vertex) != rg.m_delegated_vertex_degrees.end()) {
              std::uniform_int_distribution<vertex_t> dis(0, rg.m_delegated_vertex_degrees.at(next_vertex)-1);
              ASSERT_RELEASE(rg.m_delegated_vertex_degrees.at(next_vertex) > 0);
              vertex_t global_idx = dis(rg.m_rng);
              uint32_t dst = rg.owner(global_idx);
              if (dst == rg.m_comm.rank()) {
                rg.m_shortcuts_taken++;
                async_delegated_walk_step()(next_vertex, rg.local_idx(global_idx), walk_length, l, pthis);
              } else {
                rg.m_comm.async(rg.owner(global_idx),
                                    async_delegated_walk_step(),
                                    next_vertex,
                                    rg.local_idx(global_idx), walk_length, l, pthis);
              }
            } else {
              if (rg.m_adj_lists.owner(next_vertex) == rg.m_comm.rank()) {
                rg.m_shortcuts_taken++;
                auto aws = async_walk_step();
                rg.m_adj_lists.local_visit(next_vertex, aws, walk_length, l, pthis);
              } else {
                rg.m_adj_lists.async_visit(next_vertex, async_walk_step(), walk_length, l, pthis);
              }
            }
        } else {
          rg.m_local_paths_finished++;
        }    
      }
    };


    struct async_delegated_walk_step {            

      void operator()(vertex_t vertex,
                      uint32_t local_idx,
                      uint32_t walk_length,
                      uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside delegated step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        // path.insert(vertex);
        // pthis->m_comm.cout("At delegated vertex ", vertex);
        // pthis->m_cs.async_insert(vertex);
        auto& rg = *pthis;
        walk_length++;
        if (walk_length < l) {   
          vertex_t next_vertex = rg.m_local_delegated_adj_lists.at(vertex).at(local_idx); 
          // if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
          if (rg.m_delegated_vertex_degrees.find(next_vertex) != rg.m_delegated_vertex_degrees.end()) {
            std::uniform_int_distribution<vertex_t> dis(0, rg.m_delegated_vertex_degrees.at(next_vertex)-1);
            ASSERT_RELEASE(rg.m_delegated_vertex_degrees.at(next_vertex) > 0);
            vertex_t global_idx = dis(rg.m_rng);
            uint32_t dst = rg.owner(global_idx);
            if (dst == rg.m_comm.rank()) {
              rg.m_shortcuts_taken++;
              async_delegated_walk_step()(next_vertex, rg.local_idx(global_idx), walk_length, l, pthis);
            } else {
              rg.m_comm.async(rg.owner(global_idx),
                              async_delegated_walk_step(), 
                              next_vertex, 
                              rg.local_idx(global_idx), walk_length, l, pthis);
            }
          } else {
            if (rg.m_adj_lists.owner(next_vertex) == rg.m_comm.rank()) {
              rg.m_shortcuts_taken++;
              auto aws = async_walk_step();
              rg.m_adj_lists.local_visit(next_vertex, aws, walk_length, l, pthis);
            } else {
              rg.m_adj_lists.async_visit(next_vertex, async_walk_step(), walk_length, l, pthis);
            }
            // rg.m_adj_lists.async_visit(next_vertex, async_walk_step(), walk_length, l, pthis);
          }
        } else {
          rg.m_local_paths_finished++;
        }    
      }
    };

    
    vertex_t bias_sample_edge(std::vector<alias_table_item>& alias_table) {
      // Sample outgoing edge of v. Should be called by process which owns v via 1D partitioned
      alias_table_item itm = select_randomly_from_vec(alias_table.begin(), alias_table.end(), m_rng); 
      vertex_t s;
      if (std::get<0>(itm) == 1) {
          s = std::get<1>(itm);
      } else {
          std::uniform_real_distribution<double> zero_one_dist(0.0, 1.0);
          double d = zero_one_dist(m_rng);
          if (d < std::get<0>(itm)) {
              s = std::get<1>(itm);
          } else {
              s = std::get<2>(itm);
          }
      }
      return s;
    }
 
    struct async_bias_delegated_walk_step {            
      //  next_vertex is the vertex sampled from curr_vertex distributed alias table 
      //  No single process owns curr_vertex, every vertex owns an equal portion of curr_vertex's 
      //  alias table thus the processor running this is code is randomly chosen. 
      void operator()(vertex_t next_vertex,
                      vertex_t curr_vertex,
                      uint32_t walk_length,
                      uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside delegated step functor on rank ", pthis->m_comm.rank(), " at vertex ", curr_vertex);
        // pthis->m_cs.async_insert(curr_vertex);
        walk_length++;
        if (walk_length < l) { 
          if (pthis->m_delegated_vertex_degrees.find(next_vertex) != pthis->m_delegated_vertex_degrees.end()) {
            pthis->m_delegated_alias_tables.async_sample(next_vertex, 
                                                         async_bias_delegated_walk_step(),
                                                         walk_length,
                                                         l,
                                                         pthis);
          } else {
            pthis->m_alias_tables.async_visit(next_vertex, async_bias_walk_step(), walk_length, l, pthis);
          }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };

    struct async_bias_walk_step {            
      // This functor is called by the process which owns the vertex argument. Vertex should not be delegated. 
      void operator()(vertex_t curr_vertex,
                        std::vector<alias_table_item> alias_table,
                        uint32_t walk_length, uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", curr_vertex);
        // pthis->m_cs.async_insert(curr_vertex); 
        walk_length++;
        if (walk_length < l && alias_table.size() > 0) {
            vertex_t next_vertex = pthis->bias_sample_edge(alias_table);
            // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", curr_vertex, " stepping to ", next_vertex);
            if (pthis->m_delegated_vertex_degrees.find(next_vertex) != pthis->m_delegated_vertex_degrees.end()) {
              // pthis->m_comm.cout("Taking delegated step");
              pthis->m_delegated_alias_tables.async_sample(next_vertex, 
                                                           async_bias_delegated_walk_step(),
                                                           walk_length,
                                                           l,
                                                           pthis);
            } else {
              pthis->m_alias_tables.async_visit(next_vertex, async_bias_walk_step(), walk_length, l, pthis);
            }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };

};
