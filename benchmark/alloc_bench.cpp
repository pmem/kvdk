#include <random>
#include <algorithm>
#include <vector>
#include <iostream>
#include "../engine/pmem_allocator.hpp"

int main()
{
	std::default_random_engine re;
	std::gamma_distribution<> dist{ 64,4 };
	std::vector<uint64_t> vec_sz;
	std::uint16_t sz_sum = 0;
	while (sz_sum < (1ull << 32))	// Allocate 4GB
	{
		uint64_t sz = static_cast<uint64_t>(dist(re));
		sz_sum += sz;
		vec_sz.push_back(sz);
	}

	for (size_t i = 0; i < 100; i++)
	{
		std::cout << vec_sz[i] << std::endl;
	}

	return 0;
}