#include <ygm/collective.hpp> 
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/random.hpp>
#include <random>
#include <RMAT_generator/helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <unordered_map>
#include <ygm/detail/ygm_ptr.hpp>

/*

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
    void read_edges_from_container(Container &cont) {
      // Need to check if for_all can be called on container. Do this later
      // For now assume its a bag of std::pair<VertexID, VertexID>
      // pthis->m_weighted;
      self_ygm_ptr_type p_this = pthis;
      cont.for_all([&p_this](VertexID v1, VertexID v2){
        std::vector<VertexID> adj_list_1 = {v2};
        std::vector<VertexID> adj_list_2 = {v1};
        auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };
        p_this->m_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
        p_this->m_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
      });
    }

    void clear_paths_finished() {
      m_comm.barrier();
      m_local_paths_finished = 0;
    }

    void async_start_path(VertexID v) { 
      m_comm.cout("Made it inside async_start_path\n");
      // uint64_t vertex = select_randomly_from_vec(local_vertices.begin(), local_vertices.end(), m_rng); 
      std::set<uint64_t>path{}; // First vertex will be added to path at beginning of functor
      // if (m_delegated_vertex_degrees.find(v) != m_delegated_vertex_degrees.end()) {
      if (1) {
        std::uniform_int_distribution<uint64_t> dis(0, m_delegated_vertex_degrees[v]-1);
        uint32_t global_idx = dis(m_rng);
        // uint32_t dest_rank = global_index % world.size();
        // uint32_t new_local_index = global_index / world.size();
      //   world.async(dest_rank, take_k_path(), p_map, vertex, std::vector<uint64_t>({new_local_index}), path, true);
      //   // world.async(dis(*ptr_rng), take_k_path(), p_map, vertex, std::vector<uint64_t>(), path, true);
      // } else {
      //   graph_adj_lists.async_visit(vertex, take_k_path(), path, false);                  
      // }
        // m_comm.async(owner(global_idx), m_delegated_step, v, local_idx(global_idx), path, 10);
        m_comm.cout("About to call async using m_delegated_step on rank ", m_comm.rank(), "\n");
        m_comm.async(1, m_delegated_step, v, local_idx(global_idx), path, 10);
      } else {
        m_comm.cout("About to call async_visit on m_step on rank ", m_comm.rank(), "\n");
        // m_comm.cout() << "m_step value: " << &m_step << std::endl;
        // m_adj_lists.async_visit(v, [](uint64_t v, std::vector<uint64_t> adj){std::cout<<"Good lambda"<<std::endl;});
        v = 1; // force async visit to execute on another rank
        m_adj_lists.async_visit(v, m_step, path, 10);
      }
    }

    uint64_t paths_finished() {
      m_comm.barrier();
      return ygm::sum(m_local_paths_finished, m_comm);
    }

    void take_n_paths(uint64_t n) {
      m_comm.cout("Made it inside take_n_paths\n");
      m_comm.barrier();
      // m_comm.cout("I am rank: ", m_comm.rank(), " and my m_step functor is at address ", &m_step);
      // m_comm.cout("I am rank: ", m_comm.rank(), " and my m_delegated_step functor is at address ", &m_delegated_step);

      std::vector<uint64_t> local_vertices; // Include delegated vertices so paths can possibly start from them  
      m_adj_lists.for_all([&local_vertices](const auto& vertex, const auto& adj_list){
        local_vertices.push_back(vertex);
      });

      uint64_t ranks_total_paths = n / m_comm.size() + (n % m_comm.size() > m_comm.rank());
      for (int p = 0; p < ranks_total_paths; p++) {
        uint64_t vertex = select_randomly_from_vec(local_vertices.begin(), local_vertices.end(), m_rng); 
        m_comm.cout() << "Starting vertex: " << vertex << std::endl;
        async_start_path(vertex);  
      }

      m_comm.barrier();
    }

    size_t num_vertices() {
      m_comm.barrier();
      return m_adj_lists.size(); // Will be different once delegated vertices have been marked
      // return ygm::sum(m_adj_lists.size(), m_comm);            
    }

  // private:
     
    uint32_t owner(uint32_t global_idx) {
      return global_idx % m_comm.size();
    }

    uint32_t local_idx(uint32_t global_idx) {
      return global_idx / m_comm.size();
    }

    struct async_step_functor {            
        RaNT_Graph* p_rg;

        async_step_functor(RaNT_Graph* _p_rg) {
            p_rg = _p_rg;
        }

        void operator()(uint64_t vertex,
                        std::vector<uint64_t> adj_list,
                        std::set<uint64_t> path, uint32_t l) {
          std::cout << "Made it inside step functor\n"; 
          p_rg->m_comm.cout("Made it inside step functor\n");
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
                std::uniform_int_distribution<uint64_t> dis(0, p_rg->m_delegated_vertex_degrees[next_vertex]-1);
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

        p_rg->m_comm.cout("Made it inside delegated step functor on rank ", p_rg->m_comm.rank(), "\n");
        path.insert(vertex);
        if (path.size() < l) { 
          uint64_t next_vertex = p_rg->m_local_delegated_adj_lists[vertex][local_idx]; 
          if (path.find(next_vertex) == path.end()) { 
            p_rg->m_cs.async_insert(vertex);
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
}; */


// template <typename VertexID, typename RNG>
template <typename UNIQUE = int>
class RaNT_Graph {

  /// using self_type          = RaNT_Graph<VertexID, RNG>;
  using self_type          = RaNT_Graph<UNIQUE>;
  using self_ygm_ptr_type  = typename ygm::ygm_ptr<self_type>;

  private:
    ygm::comm&   m_comm;
    ygm::container::map<uint64_t, std::vector<uint64_t>>  m_adj_lists;
    static self_ygm_ptr_type self_ptr;
    std::mt19937 m_rng;

  public:

    RaNT_Graph(ygm::comm &comm) : 
                  m_comm(comm),
                  m_adj_lists(comm),
                  m_rng(42)
                  {
                    self_ptr = ygm::ygm_ptr(this);
                  }

    template<typename Container>
    void read_edges_from_container(Container &cont) {
      // Need to check if for_all can be called on container. Do this later
      // For now assume its a bag of std::pair<VertexID, VertexID>
      // self_ygm_ptr_type p_this = pthis;
      cont.for_all([](uint64_t v1, uint64_t v2){
        std::vector<uint64_t> adj_list_1 = {v2};
        std::vector<uint64_t> adj_list_2 = {v1};
        auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };
        self_ptr->m_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
        self_ptr->m_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
      });
    }

    void start_walk(int l) {
      // m_comm.async(1, async_step_functor(), 1, std::vector<uint64_t>{}, l);
      m_adj_lists.async_visit(1, async_step_functor(), std::set<uint64_t>{}, l);
    }

    struct async_step_functor {            
        void operator()(uint64_t vertex,
                        std::vector<uint64_t> adj_list, std::set<uint64_t> path, uint32_t l) {
          path.insert(vertex);
          self_ptr->m_comm.cout("I am rank: ", self_ptr->m_comm.rank(), ", Vertex: ", vertex);
          if (path.size() < l) {
            uint64_t next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), self_ptr->m_rng);
            self_ptr->m_adj_lists.async_visit(next_vertex, async_step_functor(), path, l);
            // self_ptr->m_comm.async(vertex % self_ptr->m_comm.size(), async_step_functor(), vertex, adj_list, l); 
          }
        }
    };    
}; 