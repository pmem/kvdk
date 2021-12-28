#include <cassert>

#include <random>
#include <stdexcept>

#include <immintrin.h>
#include <x86intrin.h>

namespace extstd {
class fast_u64_random_engine
{
public:
    using result_type = std::uint64_t;

    inline explicit fast_u64_random_engine()
    {
        reseed();
    }

    inline explicit fast_u64_random_engine(std::uint64_t seed) : s {seed}
    {
        assert(s != 0);
    }

    inline result_type operator()()
    {
        update_state();
        return s * magic;
    }

private:
    result_type s;
    static constexpr magic = 0x2545F4914F6CDD1D;

    void reseed()
    {
        size_t failures = 0;
        while (_rdseed64_step(&s) != 1)
        {
            ++failures;
            if (failures > 10000)
                throw std::runtime_error{"Fail to seed the engine"};
        }
    }

    void update_state()
    {
        if (s == 0)
        {
            reseed();
        }
        s ^= (s >> 12);
        s ^= (s << 25); 
        s ^= (s >> 27);
    }
}
}
