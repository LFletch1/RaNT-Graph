#include <RaNT_Graph.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/io/line_parser.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>

// template<> ygm::ygm_ptr<RaNT_Graph<>> RaNT_Graph<>::pthis = NULL;
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

    ygm::container::bag<std::pair<uint64_t,uint64_t>> graph_edges(world);
    ygm::timer timer;
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    // std::string filename = "/g/g20/lancef/graphs/karate_club.txt";
    std::string filename = "/p/lustre2/havoqgtu/LiveJournal/edge.txt";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter/twitter_rv.net_1_of_288";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_uk-2007-05/uk-2007-05.edgelist_1_of_288";

    world.cout0("Reading graph edges into bag");
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&graph_edges, &world](const std::string& line) {
      std::string token1, token2; 
      uint64_t v1, v2;
      std::stringstream str_stream(line);
      str_stream >> v1;
      str_stream >> v2;
      graph_edges.async_insert(std::make_pair(v1, v2));
    }); 

    world.barrier();

    // graph_edges.for_all([&world](auto &pair){
    //   // world.cout(pair.first, ", ", pair.second);
    //   // if (pair.first == pair.second && world.rank0()) {
    //   if (world.rank0()) {
    //     world.cout(pair.first, ", ", pair.second);
    //   }
    // });
    // return 0;

    // RaNT_Graph rant_graph = RaNT_Graph(world, world.size());
    RaNT_Graph rant_graph = RaNT_Graph(world, 24);
    // RaNT_Graph<int> rant_graph2 = RaNT_Graph<int>(world);
    // RaNT_Graph<long> rant_graph2 = RaNT_Graph<long>(world);
    world.cout0("Time needed to read graph edges into bag: ", timer.elapsed());
    world.cout0("Bag size: ", graph_edges.size());
    // return 0;
    world.barrier();

    timer.reset();
    rant_graph.read_edges_from_container(graph_edges);
    // rant_graph2.read_edges_from_container(graph_edges);
    world.barrier();
    world.cout0("Total time to ingest graph from bag: ", timer.elapsed(), " seconds.");
    world.cout0("Number of vertices: ", rant_graph.num_vertices());
 
    world.cout0("Taking walks on 1st RaNT-Graph");
    world.barrier();

    timer.reset();
    // uint32_t n_paths = 100000000;
    // int p = 1000000;
    // for (int i = 1; i <= 100; i++) {
    //   rant_graph.take_n_paths(p, 21);
    //   world.cout0("Total time to take ", p*i, " paths: ", timer.elapsed(), " seconds.");
    // }
    // uint32_t n_paths = 580000000;
    uint32_t n_paths = 10000000;
    // rant_graph.take_n_paths(n_paths, 21);
    rant_graph.take_n_paths(n_paths, 21);

    world.barrier();
    world.cout0("Total time to take ", n_paths, " paths: ", timer.elapsed(), " seconds.");
    // ASSERT_RELEASE(rant_graph.paths_finished() == n_paths);

    return 0;
}