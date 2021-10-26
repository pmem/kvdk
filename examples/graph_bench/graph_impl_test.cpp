//
// Created by zhanghuigui on 2021/10/26.
//

#include "graph_impl.hpp"
#include "gtest/gtest.h"

#include <string>
#include <random>

#define EDGE_VALUE_LEN 512
#define VERTEX_VALUE_LEN 64

class GraphSimulatorTest : public testing::Test{
protected:
	GraphSimulator *graph_simulator;
	GraphOptions options;
	std::mt19937 generator;
	std::string engine_name;

	Edge FormEdgeWithVertex(const Vertex& src, const Vertex& dst, int direction) {
		Edge edge;
		edge.out_direction = direction;
		edge.src = src;
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

	virtual void TearDown() override {
		delete graph_simulator;
	}
};

TEST_F(GraphSimulatorTest, GraphSimulatorBasic) {
	assert(graph_simulator != nullptr);
	// Some vertex add test
	// ---------------------------------------------------------------------------------------
	Vertex src = FormVertexWithId(0);
	Vertex dst = FormVertexWithId(1);
	ASSERT_EQ(graph_simulator->AddVertex(src) , Status::Ok);
	ASSERT_EQ(graph_simulator->AddVertex(dst) , Status::Ok);

	Edge edge = FormEdgeWithVertex(src, dst, 0);
	ASSERT_EQ(graph_simulator->AddEdge(edge), Status::Ok);

	Vertex first_vertex, second_vertex;

	ASSERT_EQ(graph_simulator->GetVertex(0, first_vertex), Status::Ok);
	ASSERT_TRUE(first_vertex.id == 0);
	ASSERT_EQ(graph_simulator->GetVertex(1, second_vertex), Status::Ok);
	ASSERT_TRUE(second_vertex.id == 0);

	// Some Edge list test
	// ---------------------------------------------------------------------------------------
	Edge tmp_edge;
	ASSERT_EQ(graph_simulator->GetEdge(src, dst, 0, tmp_edge), Status::Ok);
	ASSERT_TRUE(tmp_edge.src == first_vertex);
	ASSERT_FALSE(tmp_edge.src == second_vertex);

	EdgeList edge_list;
	ASSERT_EQ(graph_simulator->GetAllOutEdges(src, &edge_list), Status::Ok);
	ASSERT_EQ(edge_list.Num(), 1);
	ASSERT_FALSE(edge_list.Empty());

	Vertex vertex_3rd = FormVertexWithId(2);
	Edge edge_2cd = FormEdgeWithVertex(src, vertex_3rd, 0);
	ASSERT_EQ(graph_simulator->AddEdge(edge_2cd), Status::Ok);
	edge_list.Reset();
	ASSERT_EQ(graph_simulator->GetAllOutEdges(src, &edge_list), Status::Ok);
	ASSERT_EQ(edge_list.Num(), 2);
	edge_list.Reset();
	ASSERT_EQ(graph_simulator->GetAllInEdges(dst, &edge_list), Status::Ok);
	ASSERT_EQ(edge_list.Num(), 1);
	edge_list.Reset();
	ASSERT_EQ(graph_simulator->GetAllOutEdges(vertex_3rd, &edge_list), Status::Ok);
	ASSERT_EQ(edge_list.Num(), 0);

	// Some Remove op test
	// ---------------------------------------------------------------------------------------
	ASSERT_EQ(graph_simulator->RemoveVertex(src), Status::Ok);
	ASSERT_EQ(graph_simulator->GetVertex(0, first_vertex), Status::NotFound);
	// remove in edge and get in edge should not exist
	ASSERT_EQ(graph_simulator->RemoveEdge(edge), Status::Ok);
	ASSERT_EQ(graph_simulator->GetEdge(src, dst, 0, tmp_edge), Status::NotFound);
	// We don't remove out edge ,the out edge should exist
	ASSERT_EQ(graph_simulator->GetEdge(src, dst, 1, tmp_edge), Status::Ok);
}
