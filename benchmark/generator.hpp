#include <atomic>
#include <ctype.h>
#include <random>

class Generator {
public:
  virtual uint64_t Next() = 0;
};

class UniformGenerator : public Generator {
public:
  UniformGenerator(uint64_t max_num, int scale = 64)
      : base_(max_num / scale), scale_(scale), gen_cnt_(0) {
    for (uint64_t i = 0; i < base_.size(); i++) {
      base_[i] = i + 1;
    }
    std::shuffle(base_.begin(), base_.end(), std::mt19937_64());
  }

  virtual uint64_t Next() {
    auto next = gen_cnt_.fetch_add(1, std::memory_order_relaxed);
    auto index = next % base_.size();
    auto cur_scale = (next / base_.size()) % scale_;
    return base_[index] + base_.size() * cur_scale;
  }

private:
  std::vector<uint64_t> base_;
  uint64_t scale_;
  std::atomic<uint64_t> gen_cnt_;
};