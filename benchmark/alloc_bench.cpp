#include <random>
#include <algorithm>
#include <vector>
#include <iostream>
#include <map>
#include <string>
#include <chrono>
#include "../engine/pmem_allocator.hpp"
#include "../engine/kv_engine.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"


int main()
{
	std::mt19937 re{42};
	// std::default_random_engine re;
	// Mean value of variable generated by gamma distribution is alpha * beta
	// alpha is shape parameter and beta is scale parameter.
	// For alpha = 1, gamma distribution becomes exponential distribution.
	std::gamma_distribution<> dist{ 4,256 };
	std::vector<uint64_t> vec_sz;
	std::uint64_t sz_sum = 0;
	// size_t sz_alloc = (1ull << 34);	// Allocate 16GB, segment fault.
	size_t sz_alloc = (1ull << 34);	// Allocate 16GB
	while (sz_sum < sz_alloc)	
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

	kvdk::Engine* engine;
	kvdk::Status status;
	kvdk::Configs engine_configs;
	{
		engine_configs.pmem_file_size = sz_alloc * 4;
		engine_configs.pmem_segment_blocks = 1024;
		engine_configs.pmem_block_size = 64;
		engine_configs.max_write_threads = 48;
		engine_configs.populate_pmem_space = false;
	}

	std::string engine_path{ "/mnt/pmem0/bench_allocator" };
	int sink = system(std::string{ "rm -rf " + engine_path + "\n" }.c_str());

	status = kvdk::Engine::Open(engine_path, &engine, engine_configs, stdout);
	if (status == kvdk::Status::Ok)
		printf("Successfully opened a KVDK instance.\n");
	else
		return -1;

	std::shared_ptr<kvdk::PMEMAllocator> pmem_alloc = reinterpret_cast<KVDK_NAMESPACE::KVEngine*>(engine)->pmem_allocator_;

	std::vector<kvdk::SizedSpaceEntry> vec_entries;

	std::chrono::high_resolution_clock clock;
	auto start = clock.now();
	auto end = clock.now();
	std::chrono::duration<double> elapsed_seconds;

	start = clock.now();
	for (auto sz : vec_sz)
	{
		auto entry = pmem_alloc->Allocate(sz);
		vec_entries.push_back(entry);
	}
	end = clock.now();
	elapsed_seconds = end - start;
	std::cout 
		<< "elapsed time:\t" << elapsed_seconds.count() << "s\n"
		<< "allocations:\t" << vec_sz.size() << "\n"
		<< "total size:\t" << sz_sum / (1ull << 20) << " MB\n";
	
	return 0;
}