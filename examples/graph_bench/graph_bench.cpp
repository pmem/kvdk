//
// Created by zhanghuigui on 2021/10/25.
//

#include <vector>
#include <string>
#include <cstdint>
#include <random>
#include <atomic>
#include <thread>

#include "graph_impl.hpp"
#include "options.hpp"

#include <gflags/gflags.h>

using namespace google;

DEFINE_int64(vertex_nums, 10000000, "The number of the vertexes in the simple graphdb.");
DEFINE_int64(vertex_id_range, 30000000, "The range of the vertex's id.");
DEFINE_int32(threads, 32, "The number of the threads to run the graphdb's workload.");
DEFINE_string(engine_name, "kvdk", "The name of the engine, if you want to create other"
																	 "engine, 'rocksdb', 'leveldb', 'wiredtiger' are the choices.");

DEFINE_int32(vertex_info_len, 64, "The vertex_info' length. Just describe the vertex property");
DEFINE_int32(edge_info_len, 128, "The edge_info content length.The relationship between vertex");

DEFINE_int32(degree_level, 2, "The depth of the search in the simple graphdb.");
DEFINE_int32(degree_nums, 128, "The number of the begin vertexes when use degree search.");

std::mt19937_64 generator;
std::atomic<int64_t> vertex_ops;
GraphSimulator* graph_simulator{nullptr};

static void StartThreads(std::function<void()> func, int32_t thread_nums) {
	std::vector<std::thread> ths;
	for (int i = 0; i < thread_nums; i++)
		ths.emplace_back(func, i);

	for (auto& t : ths)
		t.join();
}

static Vertex CreateVertex(const uint64_t& id, const int32_t& len) {
	return Vertex(id, std::string(len, id % 26 + 'a'));
}

static Edge CreateEdge(const Vertex& in, const Vertex& out, const int32_t& len) {
	Edge edge;
	edge.src = in;
	edge.dst = out;
	edge.edge_info = std::string(len, in.id % 26 + 'b');
	return edge;
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

		Edge edge = CreateEdge(src, dst, FLAGS_edge_info_len);
		s = graph_simulator->AddEdge(edge);
		assert(s != Status::Ok);

		current_vertexes -= 2;
	}
}

void GraphDataSearchWithDegree() {
	int32_t target_level = FLAGS_degree_level;
	int32_t element_nums = FLAGS_degree_nums;

	std::vector<Vertex> elements;

	for (int i = 0;i < element_nums; i++) {
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
	fprintf(stdout,"Success bfs search count : %d\n", correct);
}

int main(int argc, char* argv[]) {
	return 0;
}



