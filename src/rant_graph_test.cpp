#include<RaNT_Graph.hpp>
#include<ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/io/line_parser.hpp>
#include <ygm/random.hpp>

template<> ygm::ygm_ptr<RaNT_Graph<>> RaNT_Graph<>::pthis = NULL;
// template<> ygm::ygm_ptr<RaNT_Graph<long>> RaNT_Graph<long>::pthis = NULL;

int main(int argc, char** argv) {

    ygm::comm world(&argc, &argv);
    /*
    int seed = 43;
    ygm::default_random_engine<> rng = ygm::default_random_engine<>(world, seed);
    using ygm_rng = ygm::default_random_engine<>;
    */
    // RaNT_Graph<uint64_t, ygm_rng> rant_graph = RaNT_Graph<uint64_t, ygm_rng>(world, rng);
    // MyObject *MyClass::myObject;
    // RaNT_Graph 
    RaNT_Graph rant_graph = RaNT_Graph(world);
    // RaNT_Graph<int> rant_graph = RaNT_Graph<int>(world);
    // RaNT_Graph<long> rant_graph2 = RaNT_Graph<long>(world);

    ygm::container::bag<std::pair<uint64_t,uint64_t>> graph_edges(world);
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    std::string filename = "/g/g20/lancef/graphs/karate_club.txt";
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&graph_edges](const std::string& line) {
      std::string token1, token2; 
      size_t pos = line.find(" ");
      if (pos != std::string::npos) {
          std::string token1 = line.substr(0, pos);
        //   std::string token2 = line.substr(pos + delim.length());
          std::string token2 = line.substr(pos + 1);
          std::stringstream converter1(token1);
          std::stringstream converter2(token2);
          uint64_t v1, v2;
          converter1 >> v1;
          converter2 >> v2;
          graph_edges.async_insert(std::make_pair(v1, v2));
      }
    }); 

    rant_graph.read_edges_from_container(graph_edges);
    // rant_graph2.read_edges_from_container(graph_edges);
    world.barrier();
 
    world.cout0("Taking walks on 1st RaNT-Graph");
    world.barrier();

    rant_graph.take_n_paths(4, 10);
    // if (world.rank0()) {
    //   rant_graph.start_walk(10); 
    // }

    // world.barrier();
    // world.cout0("Now taking walks on 2nd RaNT-Graph");
    // if (world.rank0()) {
    //   rant_graph2.start_walk(10); 
    // }
    // world.cout() << "I am rank: " << world.rank() << std::endl;   
    
    /*
    ygm::container::bag<std::pair<uint64_t,uint64_t>> graph_edges(world);
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    std::string filename = "/g/g20/lancef/graphs/test_tabs.txt";
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&graph_edges](const std::string& line) {
      std::string token1, token2; 
      size_t pos = line.find(" ");
      if (pos != std::string::npos) {
          std::string token1 = line.substr(0, pos);
        //   std::string token2 = line.substr(pos + delim.length());
          std::string token2 = line.substr(pos + 1);
          std::stringstream converter1(token1);
          std::stringstream converter2(token2);
          uint64_t v1, v2;
          converter1 >> v1;
          converter2 >> v2;
          graph_edges.async_insert(std::make_pair(v1, v2));
      }
    }); 

    rant_graph.read_edges_from_container(graph_edges);

    world.cout0() << "Number of vertices in RaNT-Graph: " << rant_graph.num_vertices() << std::endl;

    rant_graph.take_n_paths(1);

    world.cout0() << "Number of paths finished: " << rant_graph.paths_finished() << std::endl;
    */

    return 0;
}