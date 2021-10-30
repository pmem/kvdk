//
// Created by zhanghuigui on 2021/10/25.
//

#include <gflags/gflags.h>
#include <sys/time.h>

#include <atomic>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "graph_impl.hpp"
#include "options.hpp"

using namespace google;

DEFINE_bool(construct, true, "Construct the graphdb.");
DEFINE_bool(search_degree, false, "Use the bfs search degree algo.");
DEFINE_bool(topn, false, "Use the topn algo.");
DEFINE_int32(topn_num, 10, "The top num of the search with topn!");

DEFINE_int64(vertex_nums, 10000000,
             "The number of the vertexes in the simple graphdb.");
DEFINE_int64(vertex_id_range, 30000000, "The range of the vertex's id.");
DEFINE_int32(threads, 32,
             "The number of the threads to run the graphdb's workload.");
DEFINE_string(engine_name, "kvdk",
              "The name of the engine, if you want to create other"
              "engine, 'rocksdb', 'leveldb', 'wiredtiger' are the choices.");

DEFINE_int32(vertex_info_len, 64,
             "The vertex_info' length. Just describe the vertex property");
DEFINE_int32(edge_info_len, 128,
             "The edge_info content length.The relationship between vertex");

DEFINE_int32(degree_level, 2, "The depth of the search in the simple graphdb.");
DEFINE_int32(degree_nums, 128,
             "The number of the begin vertexes when use degree search.");

DEFINE_string(topn_collection, "kvdk_collection",
              "The topn collection in kvdk which will be used for sorted "
              "skiplist build.");

std::mt19937_64 generator;
std::atomic<int64_t> vertex_ops;
std::unordered_map<std::string, double> bench_timer;
GraphSimulator* graph_simulator{nullptr};

static Vertex CreateVertex(const uint64_t& id, const int32_t& len) {
  return Vertex(id, std::string(len, id % 26 + 'a'));
}

static Edge CreateEdge(const Vertex& in, const Vertex& out,
                       const uint32_t& direction, const int32_t& len) {
  Edge edge;
  edge.src = in;
  edge.dst = out;
  edge.out_direction = direction;
  edge.edge_info = std::string(len, in.id % 26 + 'b');
  return edge;
}

double NowSecs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<double>(tv.tv_usec / 1000000) + tv.tv_sec;
}

class Timer {
 public:
  Timer(const std::string& name) : start_(NowSecs()), bench_name_(name) {}
  ~Timer() { bench_timer.insert({bench_name_, NowSecs() - start_}); }

 private:
  double start_;
  std::string bench_name_;
};

static void StartThreads(std::function<void()> func, int32_t thread_nums) {
  std::vector<std::thread> ths;
  for (int i = 0; i < thread_nums; i++) ths.emplace_back(func);

  for (auto& t : ths) t.join();
}

void GraphDataConstruct() {
  assert(FLAGS_threads != 0);
  int64_t current_vertexes = vertex_ops / FLAGS_threads;

  while (current_vertexes > 0) {
    Status s;
    uint64_t first_id, second_id;
    int32_t vertex_info_len = FLAGS_vertex_info_len;
    int32_t vertex_id_range = FLAGS_vertex_id_range;
    first_id = generator();
    second_id = generator();
    if (vertex_id_range) {
      first_id = first_id % vertex_id_range;
      second_id = second_id % vertex_id_range;
    }

    Vertex src = CreateVertex(first_id, vertex_info_len);
    Vertex dst = CreateVertex(second_id, vertex_info_len);

    s = graph_simulator->AddVertex(src);
    assert(s != Status::Ok);
    s = graph_simulator->AddVertex(dst);
    assert(s != Status::Ok);

    Edge out_edge = CreateEdge(src, dst, 1, FLAGS_edge_info_len);
    s = graph_simulator->AddEdge(out_edge);
    assert(s != Status::Ok);
    Edge in_edge = CreateEdge(src, dst, 0, FLAGS_edge_info_len);
    s = graph_simulator->AddEdge(in_edge);
    assert(s != Status::Ok);

    current_vertexes -= 2;
  }
}

void GraphDataSearchWithDegree() {
  int32_t target_level = FLAGS_degree_level;
  int32_t element_nums = FLAGS_degree_nums;

  std::vector<Vertex> elements;

  for (int i = 0; i < element_nums; i++) {
    Vertex v;
    auto s = graph_simulator->GetVertex(generator(), v);
    while (s != Status::Ok) {
      s = graph_simulator->GetVertex(generator(), v);
    }
    elements.emplace_back(v);
  }

  std::vector<Status> status;
  int32_t correct = 0;
  graph_simulator->BFSSearch(elements, target_level, &status);
  for (auto& state : status) {
    if (state == Status::Ok) {
      correct++;
    }
  }
  fprintf(stdout, "Success bfs search count : %d\n", correct);
}

void GraphDataTopN() {
  assert(FLAGS_topn);
  std::vector<std::pair<Vertex, uint64_t>> topn_res;
  auto s = graph_simulator->GetTopN(topn_res, FLAGS_topn_num);
  if (s != Status::Ok) {
    fprintf(stdout, "Get the GraphData TopN Failed.");
    return;
  }

  fprintf(stdout, "TopN: \n");
  for (auto& item : topn_res) {
    fprintf(stdout, "vertex : %llu  InEdge : %llu", item.first.id, item.second);
  }
}

void LatencyPrint() {
  if (bench_timer.empty()) {
    fprintf(stdout, "bench_timer is empty, please check your workload.");
    return;
  }

  for (auto& timer : bench_timer) {
    fprintf(stdout, "benchmark : %s  timeval : %f s\n", timer.first.c_str(),
            timer.second);
  }
}

int main(int argc, char* argv[]) {
  bench_timer.clear();
  int32_t threads = FLAGS_threads;
  if (FLAGS_construct) {
    StartThreads(GraphDataConstruct, threads);
  }

  if (FLAGS_search_degree) {
    Timer timer("GraphDataSearchWithDegree");
    GraphDataSearchWithDegree();
  }

  if (FLAGS_topn) {
    Timer timer("GraphDataTopN");
    GraphDataTopN();
  }
  LatencyPrint();
  return 0;
}
