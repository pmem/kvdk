/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <assert.h>

#include <cstdint>
#include <string>
#include <vector>

#include "graph_algorithm/top_n.hpp"
#include "kv_engines/KVEngine.hpp"
#include "kv_engines/engine_factory.hpp"
#include "options.hpp"

struct Vertex {
  uint64_t id;
  std::string vertex_info;

  Vertex() : id(0) {}
  Vertex(const uint64_t& v) : id(v) {}
  Vertex(const uint64_t& v, const std::string& info)
      : id(v), vertex_info(info) {}

  bool operator==(const Vertex& v) const {
    return (id == v.id) && (vertex_info == v.vertex_info);
  }

  bool operator<(const Vertex& v) const {
    if (id != v.id) {
      return id < v.id;
    } else {
      return vertex_info < v.vertex_info;
    }
  }

  uint32_t Size() const { return 4 + 8 + vertex_info.size(); }
  void EncodeTo(std::string* output) const;
  void DecodeFrom(std::string* input);
};

struct Edge {
  uint32_t weight{0UL};
  Vertex src;  // source vertex
  Vertex dst;  // destination vertex

  uint32_t out_direction;  // decide the current edge's direction
                           // 0 : src <-- dst
                           // 1 : src --> dst
                           // 2 : src -- dst
  std::string edge_info;   // The property of this edge.

  Edge() : weight(0), out_direction(1) {}
  Edge(uint64_t src_id, uint64_t dst_id)
      : src(src_id), dst(dst_id), out_direction(1) {}
  Edge(uint32_t w, const Vertex& first, const Vertex& second,
       uint32_t direction, const std::string& info)
      : weight(w),
        src(first),
        dst(second),
        out_direction(direction),
        edge_info(info) {}

  size_t Size() const {
    return src.Size() + dst.Size() + 4 + 4 + edge_info.size();
  }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(std::string* input);
};

inline std::string VertexKeyEncode(const Vertex& v) {
  return std::to_string(v.id);
}
inline std::string VertexValueEncode(const Vertex& v) { return v.vertex_info; }

inline std::string OutEdgeKeyEncode(const Vertex& src) {
  std::string output;
  output.reserve(src.Size() + 2);
  src.EncodeTo(&output);
  output.append("_O");
  return output;
}

inline std::string InEdgeKeyEncode(const Vertex& src) {
  std::string output;
  output.reserve(src.Size() + 2);
  src.EncodeTo(&output);
  output.append("_I");
  return output;
}

inline std::string NoDirectionKeyEncode(const Vertex& src) {
  std::string output;
  output.reserve(src.Size() + 2);
  src.EncodeTo(&output);
  output.append("_N");
  return output;
}

inline Vertex EdgeKeyDecode(std::string input) {
  Vertex vertex;
  vertex.DecodeFrom(&input);
  return vertex;
}

inline bool CheckInEdgeKey(const std::string& input) {
  std::string buffer(input);
  size_t size = input.size();
  buffer.erase(0, size - 2);
  return buffer == "_I";
}

inline bool CheckOutEdgeKey(const std::string& input) {
  std::string buffer(input);
  size_t size = input.size();
  buffer.erase(0, size - 2);
  return buffer == "_O";
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

  Status AddEdge(const Edge& edge);
  Status AddVertex(const Vertex& vertex);

  Status GetVertex(const uint64_t& id, Vertex& vertex);
  Status GetEdge(const Vertex& in, const Vertex& out, int direction,
                 Edge& edge);
  Status GetAllOutEdges(const Vertex& src, EdgeList* edge_list);
  Status GetAllInEdges(const Vertex& dst, EdgeList* edge_list);

  Status RemoveVertex(const Vertex& vertex);
  Status RemoveEdge(const Edge& edge);

  // Get the top n vertex and it's in edge list nums.
  Status GetTopN(std::vector<std::pair<Vertex, uint64_t>>& top_n_vertexes,
                 int k);

  // Get the input_vertexes' n depth relationship by breadth first search.
  //
  // The reason why don't put the function into graph_algorithm directory
  // is that bfs search need the graph data map. Just put the function in
  // there ,we can use the graph data map.
  //
  // TODO(zhg) get the all degree element back for users.
  void BFSSearch(const std::vector<Vertex>& input_vertexes, int n_depth,
                 std::vector<Status>* status);

 private:
  Status AddInternalEdge(const Edge& edge);
  Status RemoveInternalEdge(const Edge& edge);
  Status BFSInternal(const Vertex& vertex, const int& n_depth);

  static inline bool CheckEdgeExists(const Vertex& src, const Vertex& dst,
                                     const uint32_t& direction,
                                     const Edge& edge) {
    return src == edge.src && dst == edge.dst &&
           direction == edge.out_direction;
  }
  GraphOptions options_;
  KVEngine* kv_engine_;
};