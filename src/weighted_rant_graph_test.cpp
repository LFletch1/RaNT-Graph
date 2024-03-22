#include <RaNT_Graph.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/io/line_parser.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>


int main(int argc, char** argv) {

    ygm::comm world(&argc, &argv);

    world.welcome();
    int seed = 43;
    ygm::default_random_engine<> rng = ygm::default_random_engine<>(world, seed);
    using ygm_rng = ygm::default_random_engine<>;

    // ygm::container::bag<std::pair<uint32_t,uint32_t>> graph_edges(world);
    ygm::timer timer;
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    // std::string filename = "/g/g20/lancef/graphs/karate_club.txt";
    std::string filename = "/p/lustre2/havoqgtu/LiveJournal/edge.txt";
    world.welcome();

    RaNT_Graph<uint32_t, ygm_rng, std::tuple<uint32_t,uint32_t,double>> weighted_rant_graph(world, world.size(), rng, true);
    RaNT_Graph<uint32_t, ygm_rng> rant_graph(world, world.size(), rng);

    world.cout0("Reading graph edges into both a weighted and unweighted RaNT-Graph");
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&](const std::string& line) {
      uint32_t v1, v2;
      std::stringstream str_stream(line);
      str_stream >> v1;
      str_stream >> v2;
    //   graph_edges.async_insert(std::make_pair(v1, v2));
      weighted_rant_graph.insert_edge_locally(std::make_tuple(v1, v2, 1.0));
      rant_graph.insert_edge_locally(std::make_pair(v1, v2));
    }); 

    world.barrier();
    
    world.cout0("Constructing weighted RaNT-Graph");
    weighted_rant_graph.construct_graph();

    world.cout0("Constructing unweighted RaNT-Graph");
    rant_graph.construct_graph();

    world.barrier();

    timer.reset();
    world.barrier();

    world.cout0("Number of vertices: ", rant_graph.num_vertices()); 
    world.cout0("Taking walks on RaNT-Graph");

    world.barrier();

    timer.reset();
    uint32_t n_walks = 100000;
    uint32_t walk_length = 50;
    rant_graph.take_n_walks(n_walks, walk_length);
    // rant_graph.take_n_paths(n_walks, 50);

    world.barrier();

    world.cout0("Total time to take ", n_walks, " walks of length ",  walk_length, ": ", timer.elapsed(), " seconds.");

    ASSERT_RELEASE(rant_graph.paths_finished() == n_walks);
    
    uint64_t expected_total_steps = n_walks * walk_length;
    uint64_t actual_steps = rant_graph.m_cs.count_all();

    // rant_graph.m_cs.for_all([&](uint32_t v, uint32_t count){
    //   world.cout("Vertex: ", v, ", Visit Count: ", count);
    // });

    // Only applies to walks where vertex visits are recorded
    ASSERT_RELEASE(expected_total_steps == actual_steps);

    return 0;
}