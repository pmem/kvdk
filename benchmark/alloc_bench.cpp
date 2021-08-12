#include <random>
#include <algorithm>
#include <vector>
#include <iostream>
#include <map>
#include <string>
#include <chrono>
#include "../engine/pmem_allocator.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"


int main()
{
	std::default_random_engine re;
	// Mean value of variable generated by gamma distribution is alpha * beta
	// alpha is shape parameter and beta is scale parameter.
	// For alpha = 1, gamma distribution becomes exponential distribution.
	std::gamma_distribution<> dist{ 4,256 };
	std::vector<uint64_t> vec_sz;
	std::uint64_t sz_sum = 0;
	size_t sz_alloc = (1ull << 32);
	while (sz_sum < sz_alloc)	// Allocate 4GB
	{
		uint64_t sz = static_cast<uint64_t>(dist(re));
		sz_sum += sz;
		vec_sz.push_back(sz);
	}
	// Print the histogram
	if (false)
	{
		std::vector<int> histogram(20);
		std::vector<uint64_t> large;
		size_t width = 256;
		for (auto sz : vec_sz)
		{
			size_t i = sz / width;
			if (i >= histogram.size())
				large.push_back(sz);
			else
				++histogram[i];
		}
		std::sort(large.begin(), large.end());
		for (size_t i = 0; i < histogram.size(); i++)
		{
			std::cout
				<< i * width << "-" << (i + 1) * width << "\t"
				<< histogram[i] << std::endl;
		}
		for (auto sz : large)
			std::cout << sz << std::endl;
	}

	const char* pmem_path = "/mnt/pmem0/bench_allocator";
	kvdk::Status status;
	kvdk::Configs engine_configs;
	{
		engine_configs.pmem_file_size = sz_alloc;
		engine_configs.pmem_segment_blocks = 1024;
		engine_configs.pmem_block_size = 64;
		engine_configs.max_write_threads = 48;
	}

	kvdk::PMEMAllocator pmem_alloc
	{
		pmem_path,
		engine_configs.pmem_file_size,
		engine_configs.pmem_segment_blocks,
		engine_configs.pmem_block_size,
		engine_configs.max_write_threads
	};

	std::vector<kvdk::SizedSpaceEntry> vec_entries;
	std::chrono::high_resolution_clock clock;
	auto start = clock.now();
	auto end = clock.now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

	start = clock.now();
	for (auto sz : vec_sz)
	{
		auto entry = pmem_alloc.Allocate(sz);
		vec_entries.push_back(entry);
	}
	end = clock.now();
	elapsed_seconds = end - start;
	std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
	
	return 0;
}