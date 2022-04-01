/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "graph_impl.hpp"

#include "coding.hpp"

void EdgeList::EdgesListEncode(std::string* result) {
  assert(!Empty());
  // Reserve the Num edges' size for the result , or the
  // string will be expand space frequently.
  result->reserve(Num() * edges[0].Size() + 8);
  PutFixed64(result, Num());
  for (size_t i = 0; i < Num(); i++) {
    edges[i].EncodeTo(result);
  }
}

Status EdgeList::EdgeListDecode(std::string* input) {
  uint64_t edges_num;
  GetFixed64(input, &edges_num);

  // We have the edge's DecodeFrom, so it's easy to decode the edge
  // from the encoded edgelist.
  for (size_t i = 0; i < edges_num; i++) {
    Edge edge;
    auto s = edge.DecodeFrom(input);
    if (s != Status::Ok) {
      SimpleLoger("EdgeList::EdgeListDecode failed.");
      return Status::Abort;
    }
    edges.emplace_back(edge);
  }

  if (input->size() > 0) {
    SimpleLoger("EdgeList::EdgeListDecode failed for input is longer.");
    return Status::Abort;
  }
  return Status::Ok;
}

void Edge::EncodeTo(std::string* output) const {
  // Total edge size : src + dst + out_direction + weight + edge_info
  uint32_t edge_size = src.Size() + dst.Size() + 4 + 4 + edge_info.size();

  // Edge encode :
  // 1. the whole edge's size
  // 2. src vertex
  // 3. fixed weight
  // 4. fixed out_direction
  // 5. dst vertex
  // 6. edge_info
  PutFixed32(output, edge_size);
  src.EncodeTo(output);
  PutFixed32(output, weight);
  PutFixed32(output, out_direction);
  dst.EncodeTo(output);
  output->append(edge_info);
}

Status Edge::DecodeFrom(std::string* input) {
  if (input->size() <= 8) {
    SimpleLoger("Edge::DecodeFrom failed.");
    return Status::Abort;
  }

  uint32_t edge_size;
  std::string edge_input;

  GetFixed32(input, &edge_size);
  edge_input = input->substr(0, edge_size);

  src.DecodeFrom(&edge_input);
  GetFixed32(&edge_input, &weight);
  GetFixed32(&edge_input, &out_direction);
  dst.DecodeFrom(&edge_input);
  edge_info = edge_input;

  // Decode finished, need truncate the input string for other edge decode
  input->erase(0, edge_size);
  return Status::Ok;
}

// vertex size(4bit)  ---- vertex_id (8bit) ---- vertex_info (string)
void Vertex::EncodeTo(std::string* output) const {
  // vertex size + id + vertex_info
  uint32_t vertex_size = 8 + vertex_info.size();
  PutFixed32(output, vertex_size);
  PutFixed64(output, id);
  output->append(vertex_info);
}

// DecodeFrom not only decode a vertex data, But also remove the
// Vertex data from the input string.
void Vertex::DecodeFrom(std::string* input) {
  // input size : vertex size(uint32) + id(uint64)
  assert(input->size() >= 12);

  uint32_t vertex_size;
  std::string vertex_input;

  GetFixed32(input, &vertex_size);
  vertex_input = input->substr(0, vertex_size);
  GetFixed64(&vertex_input, &id);
  vertex_info = vertex_input;

  input->erase(0, vertex_size);
}

GraphSimulator::GraphSimulator(const std::string& db_name, GraphOptions opts) {
  options_ = opts;
  // Init the kv_engines map
  Initial();
  // Create the engine by factory and open the engine
  kv_engine_ = CreateEngineByName::Create(db_name);
  assert(kv_engine_ != nullptr);
  SimpleLoger("Create Engine success with " + db_name);
}
GraphSimulator::~GraphSimulator() { delete kv_engine_; }

Status GraphSimulator::AddVertex(const Vertex& vertex) {
  std::string key = VertexKeyEncode(vertex);
  std::string value = VertexValueEncode(vertex);

  return kv_engine_->Put(key, value);
}

Status GraphSimulator::GetVertex(const uint64_t& id, Vertex& vertex) {
  std::string value;
  auto s = kv_engine_->Get(std::to_string(id), &value);
  if (s != Status::Ok) return s;

  vertex.id = id;
  vertex.vertex_info = value;
  return s;
}

// Check if the edge is in edge or out edge, we need to distinguish the
// direction For detail steps:
//   1. Get the edge list
//   2. Check the vertex of the input
Status GraphSimulator::GetEdge(const Vertex& in, const Vertex& out,
                               int direction, Edge& edge) {
  if (direction > 2 || direction < 0) {
    return Status::Abort;
  }

  std::string key, value;
  EdgeList edge_list;
  bool found = false;

  if (direction == 1) {
    key = OutEdgeKeyEncode(in);
    auto s = kv_engine_->Get(key, &value);
    if (s != Status::Ok) return s;
    edge_list.EdgeListDecode(&value);

    for (auto& item : edge_list.edges) {
      if ((found = CheckEdgeExists(in, out, direction, item))) edge = item;
      break;
    }
    if (!found) return Status::NotFound;
    return Status::Ok;
  }

  // deal the in direction
  key = InEdgeKeyEncode(out);
  auto s = kv_engine_->Get(key, &value);
  if (s != Status::Ok) return s;

  edge_list.EdgeListDecode(&value);
  for (auto& item : edge_list.edges) {
    if ((found = CheckEdgeExists(in, out, direction, item))) edge = item;
    break;
  }
  if (!found) return Status::NotFound;
  return Status::Ok;
}

Status GraphSimulator::GetAllInEdges(const Vertex& dst, EdgeList* edge_list) {
  std::string key, value;
  key = InEdgeKeyEncode(dst);

  auto s = kv_engine_->Get(key, &value);
  if (s != Status::Ok) return s;

  return edge_list->EdgeListDecode(&value);
}

Status GraphSimulator::GetAllOutEdges(const Vertex& src, EdgeList* edge_list) {
  std::string key, value;
  key = OutEdgeKeyEncode(src);

  auto s = kv_engine_->Get(key, &value);
  if (s != Status::Ok) return s;

  return edge_list->EdgeListDecode(&value);
}

Status GraphSimulator::RemoveVertex(const Vertex& vertex) {
  return kv_engine_->Delete(VertexKeyEncode(vertex));
}

Status GraphSimulator::RemoveEdge(const Edge& edge) {
  assert(edge.out_direction <= 2);
  return RemoveInternalEdge(edge);
}

Status GraphSimulator::RemoveInternalEdge(const Edge& edge) {
  std::string key;
  switch (edge.out_direction) {
    case 0:
      key = InEdgeKeyEncode(edge.dst);
      break;
    case 1:
      key = OutEdgeKeyEncode(edge.src);
      break;
    case 2:
      key = NoDirectionKeyEncode(edge.src);
      break;
    default:
      SimpleLoger("RemoveInternalEdge error, the direction is wrong.");
      break;
  }
  return kv_engine_->Delete(key);
}

Status GraphSimulator::AddEdge(const Edge& edge) {
  assert(edge.out_direction <= 2);
  // edge's direction is important. If the direction < 2, for every edge's two
  // vertex, we need to store two vertexes : src --> dst  and dst --> src. Their
  // key and value's data is different in kuaishou's graph storage.
  return AddInternalEdge(edge);
}

// Several steps:
// 1. format a key with src vertex
// 2. get the edgelist which is src vertex's value from engine
// 3. decode the value and add/modify the edge to edge list
// 4. format the edgelist to a new value and put into engine
Status GraphSimulator::AddInternalEdge(const Edge& edge) {
  // Check the edge is a outedge, so we could know the direction is src --> dst
  assert(edge.out_direction <= 2);
  std::string key;
  std::string value;
  EdgeList edge_list;
  bool new_edge_node = false;
  bool change_existing_edge = false;

  if (edge.out_direction == 1) {
    key = OutEdgeKeyEncode(edge.src);
  } else if (edge.out_direction == 0) {
    key = InEdgeKeyEncode(edge.dst);
  } else {
    key = NoDirectionKeyEncode(edge.src);
  }

  auto s = kv_engine_->Get(key, &value);
  if (s != Status::Ok) {
    if (s == Status::NotFound) {
      new_edge_node = true;
    } else {
      return s;
    }
  }

  if (new_edge_node) {
    edge_list.edges.emplace_back(edge);
  } else {
    edge_list.EdgeListDecode(&value);
    for (auto& item : edge_list.edges) {
      if (item.src == edge.src && item.dst == edge.dst) {
        item.weight = edge.weight;
        item.edge_info = edge.edge_info;
        change_existing_edge = true;
        break;
      }
    }
    value.clear();
    // TODO(zhg) deal the limit of edges' num.
    if (!change_existing_edge) {
      edge_list.edges.emplace_back(edge);
    }
  }

  if (edge_list.Num() > options_.max_edge_nums_per_value) {
    SimpleLoger("GraphSimulator::AddInternalEdge edgelist overflow.\n");
    return Status::Abort;
  }

  edge_list.EdgesListEncode(&value);

  // We need delete the old key
  if (!new_edge_node) {
    s = RemoveEdge(edge);
    if (s != Status::Ok) return s;
  }

  return kv_engine_->Put(key, value);
}

// The comparator for the top_n algo.
template <typename T>
struct PairCmp {
  bool operator()(const T& a, const T& b) const {
    return std::get<1>(a) >= std::get<1>(b);
  }
};

Status GraphSimulator::GetTopN(
    std::vector<std::pair<Vertex, uint64_t>>& top_n_vertexes, int k) {
  TopN<std::pair<Vertex, uint64_t>, PairCmp<std::pair<Vertex, uint64_t>>> top_n(
      k);

  auto iter = kv_engine_->NewIterator();
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    Vertex vertex;
    EdgeList edge_list;
    std::string key = iter->Key();
    std::string value = iter->Value();

    if (!CheckInEdgeKey(key)) {
      continue;
    }

    vertex = EdgeKeyDecode(key);
    edge_list.EdgeListDecode(&value);
    top_n.Push(std::make_pair(vertex, edge_list.Num()));
  }
  delete iter;
  if (top_n.Size() == 0) {
    return Status::Abort;
  }

  top_n_vertexes = top_n.Extract();
  return Status::Ok;
}

void GraphSimulator::BFSSearch(const std::vector<Vertex>& input_vertexes,
                               int n_depth, std::vector<Status>* status) {
  if (input_vertexes.empty()) return;

  for (const auto& input_vertex : input_vertexes) {
    status->emplace_back(BFSInternal(input_vertex, n_depth));
  }
}

Status GraphSimulator::BFSInternal(const Vertex& vertex, const int& n_depth) {
  std::queue<Vertex> Q;
  std::map<Vertex, bool> visited;
  Status s;

  // record the current level, and n_depth is our target level.
  int search_depth = 1;
  Q.push(vertex);
  visited[vertex] = true;
  while (!Q.empty()) {
    int size = Q.size();
    // traverse the element in the queue
    for (int ele = 0; ele < size; ele++) {
      EdgeList edge_list;
      Vertex tmp_v = Q.front();
      Q.pop();

      // Get the current vertex's out edgelists, then we could traverse
      // all over the graph vertex.
      s = GetAllOutEdges(tmp_v, &edge_list);
      if (s != Status::Ok && search_depth == 1) {
        return s;
      }

      // add the current vertex's breadth elements into queue
      for (size_t i = 0; i < edge_list.Num() && search_depth <= n_depth; i++) {
        Vertex v = edge_list.edges[i].src;
        if (visited[v]) {
          continue;
        }
        Q.push(v);
        visited[v] = true;
      }
    }
    search_depth++;
  }
  return Status::Ok;
}
