#include <ygm/collective.hpp> 
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/random.hpp>
#include <random>
#include <RMAT_generator/helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <unordered_map>

#include <ygm/detail/ygm_ptr.hpp>

template <typename VertexID, typename RNG>
class RaNT_Graph {

  public:
    using self_type          = RaNT_Graph<VertexID, RNG>;
    using self_ygm_ptr_type  = typename ygm::ygm_ptr<self_type>;

    RaNT_Graph(ygm::comm &comm, RNG rng, bool directed=false, bool weighted=false) : 
                                  m_comm(comm),
                                  m_cs(comm),
                                  pthis(this),
                                  m_rng(rng),
                                  m_weighted(weighted),
                                  m_directed(directed),
                                  m_adj_lists(comm),
                                  m_step(this),
                                  m_delegated_step(this)
                                  { }


    template<typename Container>
    void read_edges_from_container(Container cont) {
      int _;
      // Need to check if for_all can be called on container. Do this later
      // For now assume its a bag of std::pair<VertexID, VertexID>
      // pthis->m_weighted;
      cont.for_all([&pthis](VertexID v1, VertexID v2){
        std::vector<VertexID> adj_list_1 = {v2};
        std::vector<VertexID> adj_list_2 = {v1};
        auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };
        pthis->m_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
        pthis->m_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
      });
    }

    void clear_paths_finished() {
      m_comm.barrier();
      m_local_paths_finished = 0;
    }

    void take_path() {
      m_comm.barrier();
    }

  private:
     
    uint32_t owner(uint32_t global_idx) {
      return global_idx / m_comm.size();
    }

    uint32_t local_idx(uint32_t globl_idx) {
      return globl_idx % m_comm.size();
    }

    struct async_step_functor {            
        RaNT_Graph* p_rg;

        async_step_functor(RaNT_Graph* _p_rg) {
            p_rg = _p_rg;
        }

        void operator()(uint64_t vertex,
                        std::vector<uint64_t> adj_list,
                        std::set<uint64_t> path, uint32_t l) { 
          path.insert(vertex);
          p_rg->m_cs.async_insert(vertex);
          // find new unvisited neighbor to visit next
          if (path.size() < l) {
            if (adj_list.size() > p_rg->rejection_threshold) {
              bool found_unvisited = false;
              uint64_t next_vertex;
              while (!found_unvisited) {
                next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), p_rg->m_rng);
                if (path.find(next_vertex) == path.end()) {
                  found_unvisited = true;
                }
              }
              if (p_rg->m_local_delegated_adj_lists.find(next_vertex) != p_rg->m_local_delegated_adj_lists.end()) {
                std::uniform_int_distribution<uint64_t> dis(0, m_delegated_vertex_degrees[next_vertex]-1);
                uint32_t global_idx = dis(p_rg->m_rng);
                // uint32_t dest_rank = global_idx % p_rg->m_comm.size();
                // uint32_t new_local_idx = global_idx / p_rg->m_comm.size();
                // p_rg->m_comm.async(dest_rank, p_rg->m_delegated_step, next_vertex, new_local_idx, path, l);
                p_rg->m_comm.async(p_rg->owner(global_idx), p_rg->m_delegated_step, next_vertex, p_rg->local_idx(global_idx), path, l);
              } else {
                p_rg->m_adj_lists.async_visit(next_vertex, p_rg->m_step, path, l);
              }
            } else {
              std::vector<uint64_t> unvisited;
              for (uint64_t &v : adj_list) {
                if (path.find(v) == path.end()) {
                  unvisited.push_back(v);
                }
              } 
              if (unvisited.size() > 0) {
                uint64_t next_vertex = select_randomly_from_vec(unvisited.begin(), unvisited.end(), p_rg->m_rng);
                if (p_rg->m_delegated_graph_adj_lists.find(next_vertex) != p_rg->m_delegated_graph_adj_lists.end()) {
                  std::uniform_int_distribution<uint64_t> dis(0, m_delegated_vertex_degrees[next_vertex]-1);
                  uint32_t global_idx = dis(p_rg->m_rng);
                  // uint32_t dest_rank = global_idx % p_rg->m_comm.size();
                  // uint32_t new_local_idx = global_idx / p_rg->m_comm.size();
                  // p_rg->m_comm.async(dest_rank, p_rg->m_delegated_step, next_vertex, new_local_idx, path, l);
                  p_rg->m_comm.async(p_rg->owner(global_idx), p_rg->m_delegated_step, next_vertex, p_rg->local_idx(global_idx), path, l);
                } else {
                  p_rg->m_adj_lists.async_visit(next_vertex, p_rg->m_step, path, l);
                }
              } else {
                p_rg->m_local_paths_finished++;
              }
            }
          } else {
            p_rg->m_local_paths_finished++;
          }    
        }
    };
    
    struct async_delegated_step_functor {            
      RaNT_Graph* p_rg;

      async_delegated_step_functor(RaNT_Graph* _p_rg) {
          p_rg = _p_rg;
      }

      void operator()(uint64_t vertex,
                      uint64_t local_idx,
                      std::set<uint64_t> path,
                      uint32_t l) {

        path.insert(vertex);
        if (path.size() < l) { 
          uint64_t next_vertex = p_rg->m_local_delegated_adj_lists[vertex][local_idx]; 
          if (path.find(next_vertex) == path.end()) { 
            p_rg->cs.async_insert(vertex);
            if (p_rg->m_local_delegated_adj_lists.find(next_vertex) != p_rg->m_local_delegated_adj_lists.end()) {
              std::uniform_int_distribution<uint64_t> dis(0, p_rg->m_delegated_vertex_degrees[next_vertex]-1);
              uint32_t global_idx = dis(p_rg->m_rng);
              // uint32_t dest_rank = global_idx % p_rg->m_comm.size();
              // uint32_t new_local_idx = global_idx / p_rg->m_comm.size();
              // p_rg->m_comm.async(dest_rank, p_rg->m_delegated_step, next_vertex, new_local_idx, path, l);
              p_rg->m_comm.async(p_rg->owner(global_idx), p_rg->m_delegated_step, next_vertex, p_rg->local_idx(global_idx), path, l);
            } else {
              p_rg->m_adj_lists.async_visit(next_vertex, p_rg->m_step, path, l);
            }
          } else { // Vertex already in path so need to resample another neighbor from the delegated adj list
            std::uniform_int_distribution<uint64_t> dis(0, p_rg->m_delegated_vertex_degrees[vertex]-1);
            uint32_t global_idx = dis(p_rg->m_rng);
            // uint32_t dest_rank = global_idx % p_rg->m_comm.size();
            // uint32_t new_local_idx = global_idx / p_rg->m_comm.size();
            // p_rg->m_comm.async(dest_rank, p_rg->m_delegated_step, next_vertex, new_local_idx, path, l);
            p_rg->m_comm.async(p_rg->owner(global_idx), p_rg->m_delegated_step, next_vertex, p_rg->local_idx(global_idx), path, l);
          }
        } else {
          p_rg->m_local_paths_finished++;
        }    
      }
    };

 
    // Take Walk function which accepts lambda Lambda Function
    // Take walks? 
    // Build alias rejection sampling table

    // Save walks/paths

    ygm::comm&                                            m_comm;
    ygm::container::counting_set<VertexID>                m_cs;
    ygm::container::map<VertexID, std::vector<VertexID>>  m_adj_lists;
    std::unordered_map<VertexID, std::vector<VertexID>>   m_local_delegated_adj_lists;
    std::unordered_map<VertexID, uint32_t>                m_delegated_vertex_degrees;
    self_ygm_ptr_type                                     pthis;
    RNG                                                   m_rng;
    bool                                                  m_directed;   
    bool                                                  m_weighted;   
    uint64_t                                              m_local_paths_finished;
    uint32_t                                              rejection_threshold;
    async_delegated_step_functor                          m_delegated_step;
    async_step_functor                                    m_step;
};