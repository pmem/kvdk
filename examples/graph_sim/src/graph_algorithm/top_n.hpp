//
// Created by zhanghuigui on 2021/10/25.
//

#pragma once

#include <functional>
#include <queue>
#include <vector>

// A very simple TopN algorithm with the priority_queue. And the implementation
// is just store the element in the limited small heap.
//
// Only if the push element is bigger than the small heap's top ,then we pop the
// small heap and push the element in small heap. We keep the small heap's size
// in a limit, when finish the push element, the element in small heap is our
// topn.
template <class T, class Comparator = std::greater<T>>
class TopN {
 public:
  explicit TopN(size_t limit) : TopN(limit, Comparator()) {}
  TopN(size_t limit, const Comparator& cmp) : limit_(limit), cmp_(cmp) {}

  size_t Limit() const { return limit_; }
  size_t Size() const { return std::min(small_heap_.size(), limit_); }
  bool Empty() const { return Size() == 0; }
  Comparator* comparator() { return &cmp_; }
  void Reset() { small_heap_.clear(); }

  void Push(const T& v);
  inline std::vector<T> Extract() {
    std::vector<T> elements;

    while (small_heap_.size() > 0) {
      elements.emplace_back(small_heap_.top());
      small_heap_.pop();
    }
    std::reverse(elements.begin(), elements.end());
    return elements;
  }

 private:
  size_t limit_;
  Comparator cmp_;
  std::priority_queue<T, std::vector<T>, Comparator> small_heap_;
};

template <class T, class Comparator>
void TopN<T, Comparator>::Push(const T& v) {
  if (limit_ == 0) {  // There is no limit
    small_heap_.push(v);
    return;
  }

  if (small_heap_.size() < Limit()) {
    small_heap_.push(v);
  } else if (small_heap_.size() == Limit() && cmp_(small_heap_.top(), v) < 0) {
    small_heap_.pop();
    small_heap_.push(v);
  }
}

// How to use TopN class?
// Below is an example of the topn, for how to use it.
//
// template<typename T>
// struct Cmp {
// 	bool operator() (const T& a, const T& b) { return a > b; }
// };
//
// int main() {
// 	std::vector<int> arr = { 2, 7, 9, 1, 3, 5, 2, 4};
// 	TopN<int, Cmp<int>> top_n(3);
//
// 	for (int i = 0;i < arr.size(); i++) {
// 		top_n.Push(arr[i]);
// 	}
//  // The top n data.
// 	vector<int> top_n_data = top_n.Extract();
// }
