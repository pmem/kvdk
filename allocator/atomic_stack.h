#pragma once
#include <cinttypes>

namespace atomic_containers
{
	template<typename Type, uint64_t capacity=64ULL>
	class atomic_stack
	{
	public:
		atomic_stack();
		~atomic_stack();

	private:

	};

	atomic_stack::atomic_stack()
	{
	}

	atomic_stack::~atomic_stack()
	{
	}
}