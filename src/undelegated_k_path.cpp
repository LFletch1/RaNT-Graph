#include <ygm/collective.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/detail/map_impl.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/utility.hpp>
#include <ygm/random.hpp>
#include <ygm-gctc/rmat_edge_generator.hpp>
#include <ygm-gctc/helpers.hpp>
#include <ygm/io/line_parser.hpp>
#include <iterator>
#include <random>
#include <unordered_map>
#include <iostream>


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
    // uint32_t node_scale = 35;
    // uint64_t global_edge_count = 100000000000;
    distributed_rmat_edge_generator dist_edge_gen(world, vertices_scale, global_edge_count, seed); 
    dist_edge_gen.for_all([&world, &graph_adj_lists](const auto v1, const auto v2) {
        // Ensure no vertices have value of 0... 0 indicates a resampling vertex
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
    file_reader.for_all([&graph_adj_lists](const std::string& line) {
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
  }
  timer.reset(); 

  // Remove duplicates from adjacency lists
  uint64_t total_edges = 0;
  graph_adj_lists.for_all([&total_edges, &world](const auto& v, auto& adj_list) {
    std::sort( adj_list.begin(), adj_list.end() );
    adj_list.erase( std::unique( adj_list.begin(), adj_list.end() ), adj_list.end() );
    total_edges += adj_list.size();
  });
  world.barrier();

  static uint64_t finished_paths = 0;
  static uint64_t path_lengths = 0;
  struct take_k_path {
    void operator()(ygm::ygm_ptr<ygm::container::detail::map_impl<uint64_t, std::vector<uint64_t>>> pmap,
                    uint64_t vertex, std::vector<uint64_t> adj_list,
                    std::set<uint64_t> path, char l) {
      path.insert(vertex);
      ptr_cs->async_insert(vertex);
      std::uniform_int_distribution<> dis(0, ptr_world->size()-1);
      if (path.size() - 1 < l) { // Exclude first node from path length count
        // Find new unvisited neighbor to visit next
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
          pmap->async_visit(next_vertex, take_k_path(), path, l);
        } else {
          std::vector<uint64_t> unvisited;
          for (const uint64_t &v : adj_list) {
            if (path.find(v) == path.end()) {
              unvisited.push_back(v);
            }
          } 
          if (unvisited.size() > 0) {
            uint64_t next_vertex = select_randomly_from_vec(unvisited.begin(), unvisited.end(), ptr_rng);
            pmap->async_visit(next_vertex, take_k_path(), path, l);
          } else {
            finished_paths++;
            path_lengths += path.size() - 1;
          }
        }
      } else {
        finished_paths++;
        path_lengths += path.size() - 1;
      }
    }
  };

  std::vector<uint64_t> local_vertices; 
  graph_adj_lists.for_all([&local_vertices](const auto& vertex, const auto& adj_list){
    local_vertices.push_back(vertex);
  });

  world.barrier();
  std::shuffle(local_vertices.begin(), local_vertices.end(), rng);
  world.barrier();

  int my_total_paths = paths / world.size() + (paths % world.size() > world.rank());

  // Not necessary anymore becasue just randomly picking vertices in graph now 
  // ASSERT_RELEASE(paths <= graph_adj_lists.size() + delegated_graph_adj_lists.size()); 
  ASSERT_RELEASE(ygm::sum(local_vertices.size(), world) == graph_adj_lists.size());

  timer.reset();
  
  std::uniform_int_distribution<char> path_length_dis(1, k);
  for (int i = 0; i < my_total_paths; i++) { 
    uint64_t vertex = select_randomly_from_vec(local_vertices.begin(), local_vertices.end(), ptr_rng); 
    std::set<uint64_t>path({}); // First vertex will be added to path at beginning of functor
    graph_adj_lists.async_visit(vertex, take_k_path(), path, path_length_dis(rng));                  
  }

  world.barrier();
  double completion_time = timer.elapsed();

  world.cout0("=============================RESULTS================================");
  if (filename == "") {
    world.cout0("RMAT Graph Node Scale, Global Edge Count: ", vertices_scale, ", ", global_edge_count);
  } else {
    world.cout0("Graph file: ", filename);
  }
  world.cout0("Total vertices: ", graph_adj_lists.size());
  world.cout0("Total edges: ", ygm::sum(total_edges, world) / 2);
  world.cout0("K: ", k);
  world.cout0("Paths: ", paths);
  world.cout0("Rejection Threshold: ", rejection_threshold);
  world.cout0("Seed: ", seed);
  world.cout0("====================================================================");
  uint64_t total_finished_paths = ygm::sum(finished_paths, world);
  world.cout0("Finished Paths: ", total_finished_paths);
  world.cout0("Average Path Length: ", (float) ygm::sum(path_lengths, world) / (float) ygm::sum(finished_paths, world));
  world.cout0("Time (seconds): ", completion_time);
  world.cout0("Paths per Second: ", total_finished_paths / completion_time);
  world.cout0("====================================================================");
  world.barrier();

  return 0;
}
