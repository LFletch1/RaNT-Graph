#include <RaNT_Graph.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/io/line_parser.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>


int main(int argc, char** argv) {

    ygm::comm world(&argc, &argv);
    /*
    int seed = 43;
    ygm::default_random_engine<> rng = ygm::default_random_engine<>(world, seed);
    using ygm_rng = ygm::default_random_engine<>;
    */

    ygm::container::bag<std::pair<uint64_t,uint64_t>> graph_edges(world);
    ygm::timer timer;
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    // std::string filename = "/g/g20/lancef/graphs/karate_club.txt";
    // std::string filename = "/p/lustre2/havoqgtu/LiveJournal/edge.txt";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_uk-2007-05";
    std::string filename = "/p/lustre2/havoqgtu/lance/uk-2007-05_graph.txt";

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

    RaNT_Graph rant_graph = RaNT_Graph(world, world.size());
    world.cout0("Time needed to read graph edges into bag: ", timer.elapsed());
    world.cout0("Bag size: ", graph_edges.size());

    world.barrier();

    timer.reset();
    rant_graph.construct_from_edge_container(graph_edges);
    world.barrier();

    world.cout0("Total time to ingest graph from bag: ", timer.elapsed(), " seconds.");
    world.cout0("Number of vertices: ", rant_graph.num_vertices()); 
    world.cout0("Taking walks on RaNT-Graph");

    world.barrier();

    timer.reset();
    // uint32_t n_paths = 580000000;
    // uint32_t n_paths = 10000000;

    // uint32_t n_walks = 105896435;
    uint32_t n_walks = 105000000;
    // uint32_t n_walks = 1000000;
    uint32_t walk_length = 50;
    rant_graph.take_n_walks(n_walks, walk_length);
    // rant_graph.take_n_paths(n_walks, 50);



    world.barrier();

    world.cout0("Total time to take ", n_walks, " paths: ", timer.elapsed(), " seconds.");

    ASSERT_RELEASE(rant_graph.paths_finished() == n_walks);
    
    uint64_t expected_total_steps = n_walks * walk_length;
    uint64_t actual_steps = rant_graph.m_cs.count_all();

    // Only applies to walks where vertex visits are recorded
    // ASSERT_RELEASE(expected_total_steps == actual_steps);

    return 0;
}