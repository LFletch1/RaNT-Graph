#include <ygm/collective.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/detail/map_impl.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/utility.hpp>
#include <ygm/random.hpp>
#include <ygm-gctc/rmat_edge_generator.hpp>
#include <ygm/io/line_parser.hpp>
#include <iterator>
#include <random>
#include <unordered_map>
#include <iostream>


struct Params {
    uint32_t k; // Take paths of length at most k
    uint64_t p; // Number of paths to take
    uint32_t d; // Degree threshold to determine whether to delegate a vertex
    uint32_t r; // Rejection sampling threshold to determine whether to rejection sample
    uint32_t s; // Seed for randomization initialization
    uint32_t v; // Scale for number of vertices in RMAT generated graph, i.e. 2^v
    uint64_t e; // Global edge count in RMAT generated graph
    std::string f; // file or directory of json data to create graph
    std::string m;
};

Params parse_args(int argc, char *argv[], ygm::comm &comm) {
    // Defaults
    uint32_t k = 10;
    uint64_t p = 1000; 
    uint32_t d = comm.size();
    uint32_t r = 5;
    uint32_t s = 42;
    uint32_t v = 11;
    uint64_t e = 10000;
    std::string f = "";
    std::string m = "";
    Params params{k, p, d, r, s};
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            char flag = argv[i][1];
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                uint32_t value;
                uint64_t lvalue;
                if (flag == 'p' || flag == 'e') {
                  lvalue = std::atol(argv[i + 1]);
                } else if (flag != 'f' && flag != 'm') {
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
                    case 'm':
                        params.m = argv[i + 1];
                        params.m.erase(std::remove(params.m.begin(), params.m.end(), '\"'), params.m.end());
                        break;
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
      ASSERT_RELEASE(params.m != "");
    }
    return params;
}


template<typename Iter, typename RNG>
uint64_t select_randomly_from_vec(Iter start, Iter end, RNG* rng) {
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(*rng));
    return *start;
}

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
  static std::string delim = params.m;

  ASSERT_RELEASE(delegation_threshold > k);
  ASSERT_RELEASE(rejection_threshold >= k);

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
    dist_edge_gen.for_all([&world, &graph_adj_lists](const auto vertex1, const auto vertex2) {
        // Ensure no vertices have value of 0... 0 indicates a resampling vertex
        uint64_t v1 = vertex1 + 1;
        uint64_t v2 = vertex2 + 1;
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
      size_t pos = line.find(delim);
      if (pos != std::string::npos) {
          std::string token1 = line.substr(0, pos);
          std::string token2 = line.substr(pos + delim.length());
          std::stringstream converter1(token1);
          std::stringstream converter2(token2);
          uint64_t v1, v2;
          converter1 >> v1;
          converter2 >> v2;
          v1++;
          v2++;
          std::vector<uint64_t> adj_list_1 = {v2};
          std::vector<uint64_t> adj_list_2 = {v1};
          auto add_to_adjacency_list = [](const auto& key, auto& adj_list, const auto& new_adj_list) {
            adj_list.push_back(new_adj_list[0]);
          };
          graph_adj_lists.async_insert_if_missing_else_visit(v1, adj_list_1, add_to_adjacency_list);
          graph_adj_lists.async_insert_if_missing_else_visit(v2, adj_list_2, add_to_adjacency_list); 
      }
    }); 

    world.barrier();
    world.cout0("Total time to ingest graph: ", timer.elapsed(), " seconds.");
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

  static std::unordered_map<uint64_t, std::vector<uint64_t>> delegated_graph_adj_lists;
  auto delegator = [](uint64_t v, std::vector<uint64_t> delegated_adj_list) {
    delegated_graph_adj_lists.insert({v, delegated_adj_list});
  };

  for (auto &v : vertices_to_delegate) {

    auto delegate = [&world](const auto& v, auto& adj_list) {
      auto delegator = [](uint64_t v, std::vector<uint64_t> delegated_adj_list) {
        delegated_graph_adj_lists.insert({v, delegated_adj_list});
      };
      auto start = adj_list.begin();
      int total = 0;
      int adj_lists_size = adj_list.size() / world.size() + (adj_list.size() % world.size() > 0);
      for (int r = 0; r < world.size(); r++) {
        int n = adj_list.size() / world.size() + (adj_list.size() % world.size() > r);
        auto end = start + n;
        std::vector part_of_adj(start, end);
        if (adj_list.size() % world.size() > 0 && adj_list.size() % world.size() <= r) { // Add resample node to make all adjacency lists the same size
          part_of_adj.push_back(0);
        }
        ASSERT_RELEASE(part_of_adj.size() == adj_lists_size);
        world.async(r, delegator, v, part_of_adj);
        start = end;
        total += n;
      }
      ASSERT_RELEASE(total == adj_list.size());
    };
    graph_adj_lists.local_visit(v, delegate);
  }
  world.barrier();

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
                    std::set<uint64_t> path, bool delegated) {
      path.insert(vertex);
      std::uniform_int_distribution<> dis(0, ptr_world->size()-1);
      if (path.size() - 1 < k) { // Exclude first node from path length count
        if (delegated) {
          uint64_t next_vertex = select_randomly_from_vec(delegated_graph_adj_lists[vertex].begin(), delegated_graph_adj_lists[vertex].end(), ptr_rng);
          if (next_vertex != 0 && path.find(next_vertex) == path.end()) { 
            ptr_cs->async_insert(vertex);
            if (delegated_graph_adj_lists.find(next_vertex) != delegated_graph_adj_lists.end()) {
              ptr_world->async(dis(*ptr_rng), take_k_path(), pmap, next_vertex, std::vector<uint64_t>(), path, true);
            } else {
              pmap->async_visit(next_vertex, take_k_path(), path, false);
            }
          } else { // Vertex already in path so need to resample another rank in the delegated list, can maybe still check if all vertices have been selected to by pass selecting this rank again
              int other_rank = dis(*ptr_rng);
              // If other_rank == current_rank we can shortcut sending a lambda and just sample an edge in adj list again
              path.erase(vertex);
              ptr_world->async(other_rank, take_k_path(), pmap, vertex, std::vector<uint64_t>(), path, true); 
          }
        } else {
          ptr_cs->async_insert(vertex);
          // Find new unvisited neighbor to visit next
          if (adj_list.size() > rejection_threshold) {
            bool found_unvisited = false;
            uint64_t next_vertex;
            while (!found_unvisited) {
              next_vertex = select_randomly_from_vec(adj_list.begin(), adj_list.end(), ptr_rng);
              if (path.find(next_vertex) == path.end()) {
                found_unvisited = true;
              }
            }
            if (delegated_graph_adj_lists.find(next_vertex) != delegated_graph_adj_lists.end()) {
              ptr_world->async(dis(*ptr_rng), take_k_path(), pmap, next_vertex, std::vector<uint64_t>(), path, true);
            } else {
              pmap->async_visit(next_vertex, take_k_path(), path, false);
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
                ptr_world->async(dis(*ptr_rng), take_k_path(), pmap, next_vertex, std::vector<uint64_t>(), path, true);
              } else {
                pmap->async_visit(next_vertex, take_k_path(), path, false);
              }
            } else {
              finished_paths++;
              path_lengths += path.size() - 1;
            }
          }
        }
      } else {
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
  std::shuffle(local_vertices.begin(), local_vertices.end(), rng);
  world.barrier();

  int my_total_paths = paths / world.size() + (paths % world.size() > world.rank());

  // Not necessary anymore becasue just randomly picking vertices in graph now 
  // ASSERT_RELEASE(paths <= graph_adj_lists.size() + delegated_graph_adj_lists.size()); 
  ASSERT_RELEASE(ygm::sum(local_vertices.size(), world) == graph_adj_lists.size() + delegated_graph_adj_lists.size());

  timer.reset();
  
  for (int i = 0; i < my_total_paths; i++) { 
    uint64_t vertex = select_randomly_from_vec(local_vertices.begin(), local_vertices.end(), ptr_rng); 
    std::uniform_int_distribution<> dis(0, world.size()-1);
    std::set<uint64_t>path({}); // First vertex will be added to path at beginning of functor
    if (delegated_graph_adj_lists.find(vertex) != delegated_graph_adj_lists.end()) {
      world.async(dis(*ptr_rng), take_k_path(), p_map, vertex, std::vector<uint64_t>(), path, true);
    } else {
      graph_adj_lists.async_visit(vertex, take_k_path(), path, false);                  
    }
  }

  world.barrier();
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
  world.cout0("Rejection Threshold: ", rejection_threshold);
  world.cout0("Seed: ", seed);
  world.cout0("====================================================================");
  world.cout0("Finished Paths: ", ygm::sum(finished_paths, world));
  world.cout0("Average Path Length: ", (float) ygm::sum(path_lengths, world) / (float) ygm::sum(finished_paths, world));
  world.cout0("Time (seconds): ", timer.elapsed());
  world.cout0("====================================================================");
  world.barrier();

  return 0;
}
