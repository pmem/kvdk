/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "graph_impl.hpp"

#include <random>
#include <string>

#include "gtest/gtest.h"

#define EDGE_VALUE_LEN 512
#define VERTEX_VALUE_LEN 64

class GraphSimulatorTest : public testing::Test {
 protected:
  GraphSimulator* graph_simulator;
  GraphOptions options;
  std::mt19937 generator;
  std::string engine_name;
  KVEngine* kv_engine;

  Edge FormEdgeWithVertex(const Vertex& src, const Vertex& dst,
                          int32_t direction) {
    Edge edge;
    edge.src = src;
    edge.out_direction = direction;
    edge.dst = dst;
    edge.edge_info = std::string(EDGE_VALUE_LEN, generator() % 10 + '0');

    return edge;
  }

  Vertex FormVertexWithId(const uint64_t& id) {
    Vertex vertex;
    vertex.id = id;
    vertex.vertex_info = std::string(VERTEX_VALUE_LEN, generator() % 26 + 'a');
    return vertex;
  }

  virtual void SetUp() override {
    // Maybe other engines.
    engine_name = "kvdk";
    // Maybe set other options.
    options.max_edge_nums_per_value = 1000;
    graph_simulator = new GraphSimulator(engine_name, options);
  }

  virtual void TearDown() override { delete graph_simulator; }
};

TEST_F(GraphSimulatorTest, VertexDecodeAndEncode) {
  // Vertex encode and decode
  Vertex v1(1, "vertex_v1");
  std::string buffer;
  Vertex tmp;
  v1.EncodeTo(&buffer);
  tmp.DecodeFrom(&buffer);
  ASSERT_EQ(tmp.vertex_info, "vertex_v1");
  ASSERT_EQ(tmp.id, 1);
  ASSERT_TRUE(tmp == v1);
}

TEST_F(GraphSimulatorTest, EdgeEncodeAndDecode) {
  // Edge encode and decode
  Vertex v1(1, "vertex_v1");
  Vertex v2(2, "vertex_v2");
  Edge edge(2, v1, v2, 0, "v1 --> v2");
  Edge tmp_e;
  std::string buffer;

  edge.EncodeTo(&buffer);
  tmp_e.DecodeFrom(&buffer);
  ASSERT_EQ(tmp_e.weight, edge.weight);
  ASSERT_EQ(tmp_e.edge_info, edge.edge_info);
  ASSERT_EQ(tmp_e.out_direction, edge.out_direction);
  ASSERT_TRUE(tmp_e.src == edge.src);
  ASSERT_TRUE(tmp_e.dst == edge.dst);
}

TEST_F(GraphSimulatorTest, TwoEdgeDecodeAndEncode) {
  // Edge encode and decode
  Vertex v1(1, "vertex_v1");
  Vertex v2(2, "vertex_v2");
  Vertex v3(3, "vertex_v3");
  Edge edge1(2, v1, v2, 0, "v1 --> v2");
  Edge edge2(2, v1, v3, 0, "v1 --> v3");
  Edge tmp_e1, tmp_e2;
  std::string buffer;

  edge1.EncodeTo(&buffer);
  edge2.EncodeTo(&buffer);

  tmp_e1.DecodeFrom(&buffer);
  ASSERT_EQ(tmp_e1.weight, edge1.weight);
  ASSERT_EQ(tmp_e1.out_direction, edge1.out_direction);
  ASSERT_TRUE(tmp_e1.src == edge1.src);
  ASSERT_TRUE(tmp_e1.dst == edge1.dst);
  ASSERT_EQ(tmp_e1.edge_info, edge1.edge_info);
  tmp_e2.DecodeFrom(&buffer);
  ASSERT_EQ(tmp_e2.weight, edge2.weight);
  ASSERT_EQ(tmp_e2.edge_info, edge2.edge_info);
  ASSERT_EQ(tmp_e2.out_direction, edge2.out_direction);
}

TEST_F(GraphSimulatorTest, EdgeListEncodeAndDecode) {
  // Edge list encode and decode
  std::vector<Vertex> vertexes;
  EdgeList edge_list, tmp_list;
  std::string buffer;

  for (int i = 0; i <= 5; i++) {
    vertexes.emplace_back(Vertex(i, "vertex_v" + std::to_string(i)));
  }
  for (int i = 1; i <= 5; i++) {
    edge_list.edges.emplace_back(
        Edge(2, vertexes[0], vertexes[i], 0,
             vertexes[0].vertex_info + "--" + vertexes[i].vertex_info));
  }
  edge_list.EdgesListEncode(&buffer);
  tmp_list.EdgeListDecode(&buffer);
  ASSERT_FALSE(tmp_list.Empty());
  ASSERT_EQ(tmp_list.edges.size(), edge_list.edges.size());
  ASSERT_EQ(tmp_list.edges[0].src, vertexes[0]);
  for (int i = 1; i <= 5; i++) {
    ASSERT_TRUE(tmp_list.edges[i - 1].src == vertexes[0]);
    ASSERT_TRUE(tmp_list.edges[i - 1].dst == vertexes[i]);
  }
}

TEST_F(GraphSimulatorTest, GraphSimulatorBasic) {
  assert(graph_simulator != nullptr);
  // Some vertex add test
  // ---------------------------------------------------------------------------------------
  Vertex src = FormVertexWithId(0);
  Vertex dst = FormVertexWithId(1);
  ASSERT_EQ(graph_simulator->AddVertex(src), Status::Ok);
  ASSERT_EQ(graph_simulator->AddVertex(dst), Status::Ok);

  // Add the in edge and out edge
  Edge out_edge = FormEdgeWithVertex(src, dst, 1);
  ASSERT_EQ(graph_simulator->AddEdge(out_edge), Status::Ok);
  Edge in_edge = FormEdgeWithVertex(src, dst, 0);
  ASSERT_EQ(graph_simulator->AddEdge(in_edge), Status::Ok);

  Vertex first_vertex, second_vertex;

  ASSERT_EQ(graph_simulator->GetVertex(0, first_vertex), Status::Ok);
  ASSERT_EQ(first_vertex.id, 0);
  ASSERT_EQ(graph_simulator->GetVertex(1, second_vertex), Status::Ok);
  ASSERT_EQ(second_vertex.id, 1);

  // Some Edge list test
  // ---------------------------------------------------------------------------------------
  Edge tmp_edge;
  ASSERT_EQ(graph_simulator->GetEdge(src, dst, 0, tmp_edge), Status::Ok);
  ASSERT_TRUE(tmp_edge.src == src);
  ASSERT_TRUE(tmp_edge.dst == dst);

  EdgeList edge_list;
  ASSERT_EQ(graph_simulator->GetAllOutEdges(src, &edge_list), Status::Ok);
  ASSERT_EQ(edge_list.Num(), 1);
  ASSERT_FALSE(edge_list.Empty());

  Vertex vertex_3rd = FormVertexWithId(2);
  Edge edge_2cd = FormEdgeWithVertex(src, vertex_3rd, 0);
  ASSERT_EQ(graph_simulator->AddEdge(edge_2cd), Status::Ok);
  Edge edge_2cd_out = FormEdgeWithVertex(src, vertex_3rd, 1);
  ASSERT_EQ(graph_simulator->AddEdge(edge_2cd_out), Status::Ok);
  edge_list.Reset();
  ASSERT_EQ(graph_simulator->GetAllOutEdges(src, &edge_list), Status::Ok);
  ASSERT_EQ(edge_list.Num(), 2);
  edge_list.Reset();
  ASSERT_EQ(graph_simulator->GetAllInEdges(dst, &edge_list), Status::Ok);
  ASSERT_EQ(edge_list.Num(), 1);
  edge_list.Reset();
  ASSERT_NE(graph_simulator->GetAllOutEdges(vertex_3rd, &edge_list),
            Status::Ok);
  ASSERT_EQ(edge_list.Num(), 0);

  // Some Remove test
  // ---------------------------------------------------------------------------------------
  ASSERT_EQ(graph_simulator->RemoveVertex(src), Status::Ok);
  ASSERT_EQ(graph_simulator->GetVertex(0, first_vertex), Status::NotFound);
  // Remove in edge and get in edge should not exist
  ASSERT_EQ(graph_simulator->RemoveEdge(in_edge), Status::Ok);
  ASSERT_EQ(graph_simulator->GetEdge(src, dst, 0, tmp_edge), Status::NotFound);
  // We don't remove out edge ,the out edge should exist
  ASSERT_EQ(graph_simulator->GetEdge(src, dst, 1, tmp_edge), Status::Ok);

  // Remove the edge.
  ASSERT_EQ(graph_simulator->RemoveEdge(out_edge), Status::Ok);
  ASSERT_NE(graph_simulator->GetEdge(src, dst, 1, tmp_edge), Status::Ok);
  ASSERT_EQ(graph_simulator->RemoveEdge(edge_2cd), Status::Ok);
  ASSERT_EQ(graph_simulator->RemoveEdge(edge_2cd_out), Status::Ok);
}

TEST_F(GraphSimulatorTest, MemoryEngineTest) {
  engine_name = "memory";
  kv_engine = CreateEngineByName::Create(engine_name);

  ASSERT_EQ(kv_engine->Put("key1", "value1"), Status::Ok);
  ASSERT_EQ(kv_engine->Put("key2", "value2"), Status::Ok);
  ASSERT_EQ(kv_engine->Put("key3", "value3"), Status::Ok);
  ASSERT_EQ(kv_engine->Put("key4", "value3"), Status::Ok);

  std::string value;
  ASSERT_EQ(kv_engine->Get("key2", &value), Status::Ok);
  ASSERT_EQ(kv_engine->Delete("key2"), Status::Ok);
  ASSERT_EQ(kv_engine->Get("key2", &value), Status::NotFound);
  ASSERT_EQ(kv_engine->Put("key2", "value2"), Status::Ok);

  auto it = kv_engine->NewIterator();
  int i = 1;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_EQ(it->Key().c_str(), "key" + std::to_string(i));
    i++;
  }
  delete it;
  delete kv_engine;
}

TEST_F(GraphSimulatorTest, GraphSimulatorTopN) {
  // Use the simple memory engine to check the topn.
  engine_name = "memory";
  graph_simulator = new GraphSimulator(engine_name, options);

  // Edge list encode and decode
  std::vector<Vertex> vertexes;
  std::string buffer;

  for (int i = 0; i <= 5; i++) {
    vertexes.emplace_back(Vertex(i, "vertex_v" + std::to_string(i)));
  }
  for (int i = 1; i <= 5; i++) {
    Edge edge(2, vertexes[i], vertexes[0], 0,
              vertexes[0].vertex_info + "--" + vertexes[i].vertex_info);
    ASSERT_EQ(graph_simulator->AddEdge(edge), Status::Ok);
  }

  EdgeList tmp_list;
  ASSERT_EQ(graph_simulator->GetAllInEdges(vertexes[0], &tmp_list), Status::Ok);
  ASSERT_EQ(tmp_list.Num(), 5);

  std::vector<std::pair<Vertex, uint64_t>> top_n;
  ASSERT_EQ(graph_simulator->GetTopN(top_n, 1), Status::Ok);
  ASSERT_EQ(top_n.size(), 1);
  ASSERT_EQ(top_n[0].second, 5);
  ASSERT_EQ(top_n[0].first, vertexes[0]);

  for (int i = 1; i <= 5; i++) {
    if (i == 3) continue;
    Edge edge(2, vertexes[i], vertexes[3], 0,
              vertexes[1].vertex_info + "--" + vertexes[i].vertex_info);
    ASSERT_EQ(graph_simulator->AddEdge(edge), Status::Ok);
  }

  top_n.clear();
  ASSERT_EQ(graph_simulator->GetTopN(top_n, 2), Status::Ok);
  ASSERT_EQ(top_n.size(), 2);
  ASSERT_EQ(top_n[0].second, 5);
  ASSERT_EQ(top_n[0].first, vertexes[0]);
  ASSERT_EQ(top_n[1].second, 4);
  ASSERT_EQ(top_n[1].first, vertexes[3]);
}
