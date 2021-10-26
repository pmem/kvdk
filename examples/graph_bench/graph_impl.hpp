//
// Created by zhanghuigui on 2021/10/18.
//

#pragma once

#include "options.hpp"
#include "kv_engines/KVEngine.hpp"
#include "kv_engines/engine_factory.hpp"
#include "graph_algorithm/top_n.hpp"

#include <cstdint>
#include <string>
#include <vector>
#include <assert.h>
#include <kvdk/status.hpp>

struct Vertex {
	uint64_t id;
	std::string vertex_info;

	Vertex() : id(0) {}
	Vertex(const uint64_t& v) : id(v) {}
	Vertex(const uint64_t& v, const std::string& info) :
				 	id(v), vertex_info(info) {}

	bool operator==(const Vertex& v) const {
		if (id || v.id) {
			return id == v.id;
		} else {
			return vertex_info == v.vertex_info;
		}
	}

	bool operator<(const Vertex& v) {
		if (id != v.id) {
			return id < v.id;
		} else {
			return vertex_info < v.vertex_info;
		}
	}

	uint32_t Size() const { return 8 + 8 + vertex_info.size(); }
	void EncodeTo(std::string* output);
	void DecodeFrom(std::string* input);
};

struct Edge {
	Vertex src; // source vertex
	Vertex dst; // destination vertex

	uint32_t weight;
	uint32_t out_direction; // decide the current edge's direction
													// 0 : src <-- dst
													// 1 : src --> dst
													// 2 : src -- dst
	std::string edge_info; // The property of this edge.

	Edge() : weight(0), out_direction(1) {}
	Edge(uint64_t src_id, uint64_t dst_id) : src(src_id), dst(dst_id),
																					 weight(0), out_direction(1) {}
	Edge(uint32_t w, const Vertex& first, const Vertex& second,
			 uint32_t direction, const std::string& info) : weight(w),
			 src(first), dst(second), out_direction(direction), edge_info(info) {}

	void EncodeTo(std::string* output);
	Status DecodeFrom(std::string* input);
};


inline std::string VertexKeyEncode(const Vertex& v) { return std::to_string(v.id); }
inline std::string VertexValueEncode(const Vertex& v) { return v.vertex_info; }

inline std::string OutEdgeKeyEncode(const Vertex& src) {
	return std::to_string(src.id) + src.vertex_info + "_O";
}

inline std::string InEdgeKeyEncode(const Vertex& src) {
	return std::to_string(src.id) + src.vertex_info + "_I";
}

struct EdgeList {
	std::vector<Edge> edges;

	EdgeList() { edges.clear(); }
	uint64_t Num() const { return edges.size(); }
	bool Empty() const { return edges.size() == 0; }
	void Reset() { edges.clear(); }

	// Encode the current edge list to the key/value
	void EdgesListEncode(std::string* result);
	Status EdgeListDecode(std::string* input);
};

class GraphSimulator {
public:
	GraphSimulator(const std::string& db_name, GraphOptions opts);
	~GraphSimulator();

	Status AddEdge(Edge& edge);
	Status AddVertex(const Vertex& vertex);

	Status GetVertex(const uint64_t& id, Vertex& vertex);
	Status GetEdge(const Vertex& in, const Vertex& out, int direction, Edge& edge);
	Status GetAllOutEdges(const Vertex& src, EdgeList* edge_list);
	Status GetAllInEdges(const Vertex& dst, EdgeList* edge_list);

	Status RemoveVertex(const Vertex& vertex);
	Status RemoveEdge(const Edge& edge);

	// Get the top n vertex and it's in edge list nums.
	Status GetTopN(std::vector<std::pair<Vertex,int>>* top_n_vertexes);

	// Get the input_vertexes' n depth relationship by breadth first search.
	//
	// The reason why don't put the function into graph_algorithm directory
	// is that bfs search need the graph data map. Just put the function in
	// there ,we can use the graph data map.
	Status BFSSearch(const std::vector<Vertex>& input_vertexes, int n_depth);

private:
	Status AddInEdge(const Edge& edge);
	Status AddOutEdge(const Edge& edge);
	Status AddEdgeWithoutDirection(const Edge& edge);

	Status RemoveOutEdge(const Edge& edge);
	Status RemoveInEdge(const Edge& edge);

	inline bool CheckEdgeExists(const Vertex& src, const Vertex& dst,
											 const int& direction, const Edge& edge) {
		return src == edge.src && dst == edge.dst && direction == edge.out_direction;
	}
	GraphOptions options_;
	KVEngine* kv_engine_;
};