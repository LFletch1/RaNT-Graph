#include <ygm/collective.hpp> 
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/map.hpp>
#include <ygm/random.hpp>
#include <random>
#include <RMAT_generator/helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <unordered_map>
#include <ygm/detail/ygm_ptr.hpp>


// template <typename VertexID, typename RNG>
template <typename VertexID = uint32_t>
class RaNT_Graph {

  /// using self_type          = RaNT_Graph<VertexID, RNG>;
  using self_type          = RaNT_Graph<VertexID>;
  using self_ygm_ptr_type  = typename ygm::ygm_ptr<self_type>;

  private:

    ygm::comm&                                            m_comm;
    ygm::container::map<VertexID, std::vector<VertexID>>  m_adj_lists;
    self_ygm_ptr_type                                     pthis;

    std::mt19937                                          m_rng;
    std::unordered_map<VertexID, std::vector<VertexID>>   m_local_delegated_adj_lists;
    std::unordered_map<VertexID, uint64_t>                m_delegated_vertex_degrees;
    std::vector<VertexID>                                 m_local_vertices;
    // RNG                                                   m_rng;
    // bool                                                  m_directed;   
    // bool                                                  m_weighted;   
    uint64_t                                              m_local_paths_finished;
    uint32_t                                              m_delegation_threshold;
    uint32_t                                              m_rejection_threshold;

  public:

    ygm::container::counting_set<VertexID>                m_cs;

    RaNT_Graph(ygm::comm &comm, uint32_t d_thresh) : 
                  m_comm(comm),
                  m_adj_lists(comm),
                  m_rng(42),
                  m_cs(comm),
                  m_local_paths_finished(0),
                  // m_weighted(weighted),
                  // m_directed(directed) 
                  m_delegation_threshold(d_thresh),
                  pthis(this) {}

    void clear_paths_finished() {
      m_comm.barrier();
      m_local_paths_finished = 0;
    }

    void start_path(VertexID v, uint32_t l) { 

      std::set<VertexID> path{}; // First vertex will be added to path at beginning of called functor
      if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
        std::uniform_int_distribution<VertexID> dis(0, m_delegated_vertex_degrees[v]-1);
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
        VertexID vertex = select_randomly_from_vec(m_local_vertices.begin(), m_local_vertices.end(), m_rng); 
        start_path(vertex, path_length);  
      }

      m_comm.barrier();
    }


    void start_walk(VertexID v, uint32_t target_walk_length) { 

      if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
        std::uniform_int_distribution<VertexID> dis(0, m_delegated_vertex_degrees[v]-1);
        uint32_t global_idx = dis(m_rng);
        m_comm.async(owner(global_idx), 
                     async_delegated_walk_step(), 
                     v, 
                     local_idx(global_idx), 0, target_walk_length, pthis);
      } else {
        m_adj_lists.async_visit(v, async_walk_step(), 0, target_walk_length, pthis);
      }
    }


    void take_n_walks(uint64_t n, uint32_t target_walk_length) {

      uint64_t ranks_total_paths = n / m_comm.size() + (n % m_comm.size() > m_comm.rank());
      // std::uniform_int_distribution<uint32_t> dis(1, k);

      for (int p = 0; p < ranks_total_paths; p++) {
        // uint32_t path_length = dis(m_rng);
        VertexID vertex = select_randomly_from_vec(m_local_vertices.begin(), m_local_vertices.end(), m_rng); 
        start_walk(vertex, target_walk_length);  
      }

      m_comm.barrier();
    }

    uint64_t paths_finished() {
      m_comm.barrier();
      return ygm::sum(m_local_paths_finished, m_comm);
    }

    size_t num_vertices() {
      // Correct if delegation threshold > comm.size(). If not then only will be correct for rank 0.
      m_comm.barrier();
      return m_adj_lists.size() + m_delegated_vertex_degrees.size();
    }

    uint32_t owner(uint64_t vertex_id) {
      return (uint32_t) (vertex_id % m_comm.size());
    }

    uint64_t local_idx(VertexID global_idx) {
      return global_idx / m_comm.size();
    }

    template<typename Container>
    void construct_from_edge_container(Container &cont) {
      // Need to check if for_all can be called on container. Do this later
      // For now assume its a bag of std::pair<VertexID, VertexID>
      ygm::ygm_ptr local_pthis = pthis;

      cont.for_all([&](VertexID v1, VertexID v2){
        if (v1 != v2) {
          std::vector<VertexID> adj_list_1 = {v2};
          std::vector<VertexID> adj_list_2 = {v1};
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
      std::vector<VertexID> vertices_to_delegate;
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

          auto delegator = [](VertexID v, std::vector<VertexID> delegated_adj_list, uint64_t degree, self_ygm_ptr_type pthis) {
            pthis->m_local_delegated_adj_lists.insert({v, delegated_adj_list});
            pthis->m_delegated_vertex_degrees.insert({v, degree});
          };
          int total = 0;
          for (int r = 0; r < pthis->m_comm.size(); r++) {
            std::vector<VertexID> part_of_adj;
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
    }

    /*
    struct async_path_step {            
      
      void operator()(VertexID vertex,
                        std::vector<VertexID> adj_list,
                        std::set<VertexID> path, uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        path.insert(vertex);
        pthis->m_cs.async_insert(vertex);

        // find new unvisited neighbor to visit next
        if (path.size() < l) {
          // if (adj_list.size() > pthis->m_rejection_threshold) {
          if (adj_list.size() > l) {
            bool found_unvisited = false;
            VertexID next_vertex;
            while (!found_unvisited) {
              next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), pthis->m_rng);
              if (path.find(next_vertex) == path.end()) {
                found_unvisited = true;
              }
            }
            if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
              std::uniform_int_distribution<VertexID> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
              uint64_t global_idx = dis(pthis->m_rng);
              pthis->m_comm.async(pthis->owner(global_idx),
                                  async_delegated_path_step(),
                                  next_vertex,
                                  pthis->local_idx(global_idx), path, l, pthis);
            } else {
              pthis->m_adj_lists.async_visit(next_vertex, async_path_step(), path, l, pthis);
            }
          } else {
            std::vector<VertexID> unvisited;
            for (VertexID &v : adj_list) {
              if (path.find(v) == path.end()) {
                unvisited.push_back(v);
              }
            } 
            if (unvisited.size() > 0) {
              VertexID next_vertex = select_randomly_from_vec(unvisited.begin(), unvisited.end(), pthis->m_rng);
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
      
      void operator()(VertexID vertex,
                        std::vector<VertexID> adj_list,
                        uint32_t walk_length, uint32_t l, self_ygm_ptr_type pthis) {
        // pthis->m_comm.cout("Made it inside step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        // path.insert(vertex);

        // pthis->m_cs.async_insert(vertex);
        walk_length++;

        if (walk_length < l) {
            // walk_length++;
            VertexID next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), pthis->m_rng);
            if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
              std::uniform_int_distribution<uint64_t> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
              uint64_t global_idx = dis(pthis->m_rng);
              pthis->m_comm.async(pthis->owner(global_idx),
                                  async_delegated_walk_step(),
                                  next_vertex,
                                  pthis->local_idx(global_idx), walk_length, l, pthis);
            } else {
              pthis->m_adj_lists.async_visit(next_vertex, async_walk_step(), walk_length, l, pthis);
            }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };


    struct async_delegated_walk_step {            

      void operator()(VertexID vertex,
                      uint64_t local_idx,
                      uint32_t walk_length,
                      uint32_t l, self_ygm_ptr_type pthis) {

        // pthis->m_comm.cout("Made it inside delegated step functor on rank ", pthis->m_comm.rank(), " at vertex ", vertex);
        // path.insert(vertex);
        // pthis->m_cs.async_insert(vertex);
        walk_length++;

        if (walk_length < l) { 
          VertexID next_vertex = pthis->m_local_delegated_adj_lists[vertex][local_idx]; 
          if (pthis->m_local_delegated_adj_lists.find(next_vertex) != pthis->m_local_delegated_adj_lists.end()) {
            std::uniform_int_distribution<uint64_t> dis(0, pthis->m_delegated_vertex_degrees[next_vertex]-1);
            uint64_t global_idx = dis(pthis->m_rng);

            pthis->m_comm.async(pthis->owner(global_idx),
                                async_delegated_walk_step(), 
                                next_vertex, 
                                pthis->local_idx(global_idx), walk_length, l, pthis);
          } else {
            pthis->m_adj_lists.async_visit(next_vertex, async_walk_step(), walk_length, l, pthis);
          }
        } else {
          pthis->m_local_paths_finished++;
        }    
      }
    };

};
