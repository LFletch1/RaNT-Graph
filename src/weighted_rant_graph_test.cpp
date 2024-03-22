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
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter";
    world.welcome();

    RaNT_Graph<uint32_t,std::tuple<uint32_t,uint32_t,double>> weighted_rant_graph(world, world.size(), rng, true);
    // RaNT_Graph<uint32_t> rant_graph(world, world.size(), rng, false, true);

    world.cout0("Reading graph edges into both a weighted and unweighted RaNT-Graph");
    ygm::io::line_parser file_reader(world, {filename});
    file_reader.for_all([&](const std::string& line) {
      uint32_t u, v;
      std::stringstream str_stream(line);
      str_stream >> u;
      str_stream >> v;
      weighted_rant_graph.insert_edge_locally(std::make_tuple(u, v, 1.0));
      // rant_graph.insert_edge_locally(std::make_pair(u, v));
    }); 

    world.barrier();
    
    world.cout0("Constructing weighted RaNT-Graph");
    weighted_rant_graph.construct_graph();

    // world.cout0("Constructing unweighted RaNT-Graph");
    // rant_graph.construct_graph();

    // world.barrier();

    // timer.reset();
    // world.barrier();

    // world.cout0("Number of vertices unweighted graph: ", rant_graph.num_vertices()); 
    world.cout0("Number of vertices weighted graph: ", weighted_rant_graph.num_vertices()); 
    world.cout0("Taking walks on weighted RaNT-Graph");

    world.barrier();

    // timer.reset();
    uint32_t n_walks = 1000000;
    uint32_t walk_length = 50;
    // weighted_rant_graph.take_n_walks(n_walks, walk_length);

    // world.barrier();

    // world.cout0("Total time to take ", n_walks, " walks of target length ",  walk_length, ": ", timer.elapsed(), " seconds.");

    // ASSERT_RELEASE(weighted_rant_graph.paths_finished() == n_walks);
    
    // uint64_t expected_total_steps = n_walks * walk_length;
    // uint64_t actual_steps = weighted_rant_graph.m_cs.count_all();

    // world.cout0("Average walk length: ", (double) actual_steps / (double) n_walks);
    // rant_graph.m_cs.for_all([&](uint32_t v, uint32_t count){
    //   world.cout("Vertex: ", v, ", Visit Count: ", count);
    // });

    // Only applies to walks where vertex visits are recorded and on undirected graphs
    // ASSERT_RELEASE(expected_total_steps == actual_steps);

    return 0;
}