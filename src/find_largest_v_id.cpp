#include <ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/io/line_parser.hpp>
#include <ygm/utility.hpp>


int main(int argc, char** argv) {

    ygm::comm world(&argc, &argv);

    // world.welcome();

    ygm::container::bag<std::pair<uint64_t,uint64_t>> graph_edges(world);
    ygm::timer timer;
    // std::string filename = "/g/g20/lancef/graphs/facebook_combined.txt";
    // std::string filename = "/g/g20/lancef/graphs/karate_club.txt";
    // std::string filename = "/p/lustre2/havoqgtu/LiveJournal/edge.txt";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_twitter";
    // std::string filename = "/p/lustre2/havoqgtu/trevor_uk-2007-05";
    // std::string filename = "/p/lustre2/havoqgtu/lance/uk-2007-05_graph.txt";
    // std::string filename = "/p/lustre2/havoqgtu/lance/WDC12_unweighted_rnd_ids.edgelist";

    world.cout0("Reading graph edges into bag");
    ygm::io::line_parser file_reader(world, {filename});

    uint64_t max_v_id = 0;
    file_reader.for_all([&](const std::string& line) {
      uint64_t v1, v2;
      std::stringstream str_stream(line);
      str_stream >> v1;
      str_stream >> v2;
      if (v1 > max_v_id) {
        max_v_id = v1;
      }
      if (v2 > max_v_id) {
        max_v_id = v2;
      }
      graph_edges.async_insert(std::make_pair(v1, v2));
    }); 

    world.barrier();

    uint64_t global_max_id = ygm::max(max_v_id, world);

    world.cout0("Time needed to read graph edges into bag: ", timer.elapsed());
    world.cout0("LARGEST VERTEX ID: ", global_max_id);

    return 0;
}