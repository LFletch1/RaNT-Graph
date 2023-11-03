#include <ygm/collective.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/detail/map_impl.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/utility.hpp>
#include <ygm/random.hpp>
#include <ygm-gctc/rmat_edge_generator.hpp>
#include <ygm-gctc/helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <unordered_map>

int main(int argc, char** argv) {

  ygm::comm world(&argc, &argv);
  Params params = parse_args(argc, argv, world);
  world.welcome();
  world.cout0("");

  static uint32_t k = params.k;
  static uint64_t paths = params.p;
  static uint32_t delegation_threshold = params.d;
  static uint32_t rejection_threshold = params.r;
  static uint32_t seed = params.s;
  uint32_t vertices_scale = params.v;
  uint64_t global_edge_count = params.e;
  std::string filename = params.f;
  static char delim = params.m;
  world.cout0("Delim: \"", delim ,"\"");

  ASSERT_RELEASE(delegation_threshold > k);

  ygm::timer timer;
  ygm::container::detail::map_impl<uint64_t, std::vector<uint64_t>> graph_adj_lists(world);
  ygm::container::counting_set<uint64_t> paths_on_vertices(world);
  ygm::default_random_engine<> rng = ygm::default_random_engine<>(world, seed);
  auto p_map = world.make_ygm_ptr(graph_adj_lists);

  static auto* ptr_cs = &paths_on_vertices;
  static auto* ptr_world = &world;
  static auto* ptr_graph = &graph_adj_lists;
  static auto* ptr_rng = &rng;

  if (filename == "") { // Create graph using RMAT generator
    distributed_rmat_edge_generator dist_edge_gen(world, vertices_scale, global_edge_count, seed); 
    // world.cout(dist_edge_gen.m_local_generator.max_vertex_id());
    // return 0;
    dist_edge_gen.for_all([&world, &graph_adj_lists](const auto v1, const auto v2) {
        std::vector<uint64_t> adj_list_1 = {v2};
        std::vector<uint64_t> adj_list_2 = {v1};
        auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
          adj_list.push_back(new_adj_list[0]);
        };
        graph_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
        graph_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list);
    });
    world.barrier();
    world.cout0("Total time to generate RMAT graph: ", timer.elapsed(), " seconds.");
  } else { // Read graph from file
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&graph_adj_lists, &world](const std::string& line) {
      std::string token1, token2; 
      std::stringstream ss(line);
      std::getline(ss, token1, delim);
      std::getline(ss, token2, delim);
      std::stringstream converter1(token1);
      std::stringstream converter2(token2);
      // world.cout(token1, token2);
      uint64_t v1, v2;
      converter1 >> v1;
      converter2 >> v2;
      std::vector<uint64_t> adj_list_1 = {v2};
      std::vector<uint64_t> adj_list_2 = {v1};
      auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
        adj_list.push_back(new_adj_list[0]);
      };
      graph_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
      graph_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
    }); 

    world.barrier();
    world.cout0("Total time to ingest graph: ", timer.elapsed(), " seconds.");
    ASSERT_RELEASE(graph_adj_lists.size() > 0);
  }
  timer.reset(); 

  // Determine which vertices to delegate and remove duplicates from adjacency lists
  uint64_t total_edges = 0;
  std::vector<uint64_t> vertices_to_delegate;
  graph_adj_lists.for_all([&total_edges, &world, &vertices_to_delegate](const auto& v, auto& adj_list) {
    std::sort( adj_list.begin(), adj_list.end() );
    adj_list.erase( std::unique( adj_list.begin(), adj_list.end() ), adj_list.end() );
    if (adj_list.size() >= delegation_threshold) {
      vertices_to_delegate.push_back(v);
    }
    total_edges += adj_list.size();
  });

  // Delegate vertices
  static std::unordered_map<uint64_t, std::vector<uint64_t>> delegated_graph_adj_lists;
  static std::unordered_map<uint64_t, uint64_t> delegated_vertex_degrees;
  for (auto &v : vertices_to_delegate) {
    auto delegate = [&world](const auto& v, auto& adj_list) {

      auto delegator = [](uint64_t v, std::vector<uint64_t> delegated_adj_list, uint64_t degree) {
        delegated_graph_adj_lists.insert({v, delegated_adj_list});
        delegated_vertex_degrees.insert({v, degree});
      };

      int total = 0;
      int starting_rank = v % world.size();
      int ranks_delegated = 0;
      for (int ranks_delegated = 0; ranks_delegated < world.size(); ++ranks_delegated) {
        int r = (starting_rank + ranks_delegated) % world.size();
        std::vector<uint64_t> part_of_adj;
        for (int i = ranks_delegated; i < adj_list.size(); i += world.size()) {
          part_of_adj.push_back(adj_list.at(i));
        }
        total += part_of_adj.size();
        world.async(r, delegator, v, part_of_adj, adj_list.size());
      }
      ASSERT_RELEASE(total == adj_list.size());
    };
    graph_adj_lists.local_visit(v, delegate);
  }
  world.barrier();
  world.cout0("Total vertices: ", graph_adj_lists.size());

  static uint32_t total_deleted;
  for (auto &v : vertices_to_delegate) {
    graph_adj_lists.async_erase(v);
    total_deleted++;
  }
  world.barrier();
  world.cout0("Total time to delegate vertices: ", timer.elapsed(), " seconds.");

  static uint64_t finished_paths = 0;
  static uint64_t path_lengths = 0;
  struct take_k_path {
    void operator()(ygm::ygm_ptr<ygm::container::detail::map_impl<uint64_t, std::vector<uint64_t>>> pmap,
                    uint64_t vertex, std::vector<uint64_t> adj_list,
                    std::set<uint64_t> path, char l, bool delegated) {
      path.insert(vertex);
      if (path.size() - 1 < l) { // Exclude first node from path length count
        if (delegated) {
          // To save on communication data, store the index as first number in adj_list when it is delegated
          uint64_t local_index = adj_list[0];
          uint64_t next_vertex = delegated_graph_adj_lists[vertex][local_index]; 
          if (path.find(next_vertex) == path.end()) { 
            ptr_cs->async_insert(vertex);
            if (delegated_graph_adj_lists.find(next_vertex) != delegated_graph_adj_lists.end()) {
              std::uniform_int_distribution<uint64_t> dis(0, delegated_vertex_degrees[next_vertex]-1);
              uint32_t global_index = dis(*ptr_rng);
              uint32_t dest_rank = (global_index + next_vertex) % ptr_world->size(); // + next_vertex % ptr_world->size();
              uint32_t new_local_index = global_index / ptr_world->size();
              ptr_world->async(dest_rank, take_k_path(), pmap, next_vertex, std::vector<uint64_t>({new_local_index}), path, l, true);
            } else {
              pmap->async_visit(next_vertex, take_k_path(), path, l, false);
            }
          } else { // Vertex already in path so need to resample another neighbor from the delegated adj list
            std::uniform_int_distribution<uint64_t> dis(0, delegated_vertex_degrees[vertex]-1);
            uint32_t global_index = dis(*ptr_rng);
            uint32_t dest_rank = (global_index + vertex) % ptr_world->size(); // + vertex % ptr_world->size();
            uint32_t new_local_index = global_index / ptr_world->size();
            ptr_world->async(dest_rank, take_k_path(), pmap, vertex, std::vector<uint64_t>({new_local_index}), path, l, true); 
          }
        } else {
          ptr_cs->async_insert(vertex);
          // d / (d - p) = estimated samples needed to accept sample from rejection sampling
          // d = vertex degree, p = current path length 
          int worst_case_unvisted = adj_list.size() - path.size();
          if (worst_case_unvisted > 0 && (adj_list.size() / worst_case_unvisted) < rejection_threshold) { 
            bool found_unvisited = false;
            uint64_t next_vertex;
            while (!found_unvisited) {
              next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), ptr_rng);
              if (path.find(next_vertex) == path.end()) {
                found_unvisited = true;
              }
            }
            if (delegated_graph_adj_lists.find(next_vertex) != delegated_graph_adj_lists.end()) {
              std::uniform_int_distribution<uint64_t> dis(0, delegated_vertex_degrees[next_vertex]-1);
              uint32_t global_index = dis(*ptr_rng);
              uint32_t dest_rank = (global_index + next_vertex) % ptr_world->size(); // + next_vertex % ptr_world->size();
              uint32_t new_local_index = global_index / ptr_world->size();
              ptr_world->async(dest_rank, take_k_path(), pmap, next_vertex, std::vector<uint64_t>({new_local_index}), path, l, true);
            } else {
              pmap->async_visit(next_vertex, take_k_path(), path, l, false);
            }
          } else {
            std::vector<uint64_t> unvisited;
            for (const uint64_t &v : adj_list) {
              if (path.find(v) == path.end()) {
                unvisited.push_back(v);
              }
            } 
            if (unvisited.size() > 0) {
              uint64_t next_vertex = select_randomly_from_vec(unvisited.begin(), unvisited.end(), ptr_rng);
              if (delegated_graph_adj_lists.find(next_vertex) != delegated_graph_adj_lists.end()) {
                std::uniform_int_distribution<uint64_t> dis(0, delegated_vertex_degrees[next_vertex]-1);
                uint32_t global_index = dis(*ptr_rng);
                uint32_t dest_rank = (global_index + next_vertex) % ptr_world->size(); // + next_vertex % ptr_world->size();
                uint32_t new_local_index = global_index / ptr_world->size();
                ptr_world->async(dest_rank, take_k_path(), pmap, next_vertex, std::vector<uint64_t>({new_local_index}), path, l, true);
              } else {
                pmap->async_visit(next_vertex, take_k_path(), path, l, false);
              }
            } else {
              finished_paths++;
              path_lengths += path.size() - 1;
            }
          }
        }
      } else {
        ptr_cs->async_insert(vertex);
        finished_paths++;
        path_lengths += path.size() - 1;
      }
    }
  };

  std::vector<uint64_t> local_vertices = vertices_to_delegate; // Include delegated vertices so paths can possibly start from them  
  graph_adj_lists.for_all([&local_vertices](const auto& vertex, const auto& adj_list){
    local_vertices.push_back(vertex);
  });

  world.barrier();

  int my_total_paths = paths / world.size() + (paths % world.size() > world.rank());

  ASSERT_RELEASE(ygm::sum(local_vertices.size(), world) == graph_adj_lists.size() + delegated_graph_adj_lists.size());

  timer.reset();
  
  std::uniform_int_distribution<char> path_length_dis(1, k);
  for (int i = 0; i < my_total_paths; i++) { 
    char path_length = path_length_dis(rng);
    uint64_t vertex = select_randomly_from_vec(local_vertices.begin(), local_vertices.end(), ptr_rng); 
    std::set<uint64_t>path({}); // First vertex will be added to path at beginning of functor
    if (delegated_graph_adj_lists.find(vertex) != delegated_graph_adj_lists.end()) {
      std::uniform_int_distribution<uint64_t> dis(0, delegated_vertex_degrees[vertex]-1);
      uint32_t global_index = dis(rng);
      uint32_t dest_rank = (global_index + vertex) % ptr_world->size(); // + vertex % world.size();
      uint32_t new_local_index = global_index / world.size();
      world.async(dest_rank, take_k_path(), p_map, vertex, std::vector<uint64_t>({new_local_index}), path, path_length, true);
    } else {
      graph_adj_lists.async_visit(vertex, take_k_path(), path, path_length, false);                  
    }
  }

  world.barrier();
  double completion_time = timer.elapsed();

  world.cout0("=============================RESULTS================================");
  if (filename == "") {
    world.cout0("RMAT Graph Node Scale, Global Edge Count: ", vertices_scale, ", ", global_edge_count);
  } else {
    world.cout0("Graph file: ", filename);
  }
  world.cout0("Total vertices: ", graph_adj_lists.size() + delegated_graph_adj_lists.size());
  world.cout0("Total edges: ", ygm::sum(total_edges, world) / 2);
  world.cout0("K: ", k);
  world.cout0("Paths: ", paths);
  world.cout0("Delegation Threshold: ", delegation_threshold);
  world.cout0("Total delegated vertices: ", delegated_graph_adj_lists.size());
  world.cout0("Rejection Sampling Threshold: ", rejection_threshold);
  world.cout0("Seed: ", seed);
  uint64_t total_finished_paths = ygm::sum(finished_paths, world);
  world.cout0("====================================================================");
  world.cout0("Finished Paths: ", ygm::sum(finished_paths, world));
  world.cout0("Average Path Length: ", (double) ygm::sum(path_lengths, world) / (double) total_finished_paths);
  world.cout0("Time (seconds): ", completion_time);
  world.cout0("Paths per Second: ", total_finished_paths / completion_time);
  world.cout0("====================================================================");

  return 0;
}
