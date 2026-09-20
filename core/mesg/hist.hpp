// core/mesg/hist.hpp
#pragma once
#include <cstddef>
#include <memory>
#include <vector>

namespace fins::rt {

/// 历史窗口视图：N 帧的只读视图，帧在内存上不连续（各自 shared_ptr 保活）。
///
/// 约定（违反 = UB）：
///   · 借用视图：不拥有数据，仅本拍 execute() 期间有效，**不得存储**。
///   · 元素只读，不得 cast 掉 const；不保证连续（无 data()，不得对 &h[i] 做指针算术）。
///   · 空窗口下 operator[]/back()/front() 为 UB，先用 empty()/size() 判。
///
/// 安全保证：内部持 shared_ptr<const T>，payload 由引用计数保活——即使 producer 的
/// 历史槽随后淘汰该帧，视图仍有效。
template <typename T>
class History {
public:
  using value_type = T;
  using size_type = std::size_t;

  History() = default;

  /// 框架内部构造：接管 shared_ptr 数组（零 payload 拷贝）
  explicit History(std::vector<std::shared_ptr<const T>> frames)
      : frames_(std::move(frames)) {}

  // ── 只读接口（刻意最小化）──
  size_type size() const noexcept { return frames_.size(); }
  bool empty() const noexcept { return frames_.empty(); }

  /// 第 i 帧（零拷贝，返回 const T&）；i 越界 = UB（用 at() 做检查版）
  const T& operator[](size_type i) const { return *frames_[i]; }

  /// 第 i 帧（带越界检查，抛 std::out_of_range）
  const T& at(size_type i) const { return *frames_.at(i); }

  /// 最新一帧（尾部 = 最新；空窗口 = UB）
  const T& back() const { return *frames_.back(); }

  /// 最旧一帧（头部 = 最旧；空窗口 = UB）
  const T& front() const { return *frames_.front(); }

  // ── 迭代器：随机访问，但底层是 shared_ptr 数组，解引用返回 const T& ──
  class const_iterator {
    using base = typename std::vector<std::shared_ptr<const T>>::const_iterator;
    base it_{};
    friend class History;
    explicit const_iterator(base it) : it_(it) {}
  public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T*;
    using reference = const T&;

    reference operator*() const { return **it_; }
    pointer operator->() const { return it_->get(); }
    reference operator[](difference_type n) const { return *it_[n]; }

    const_iterator& operator++() { ++it_; return *this; }
    const_iterator operator++(int) { auto t = *this; ++it_; return t; }
    const_iterator& operator--() { --it_; return *this; }
    const_iterator operator--(int) { auto t = *this; --it_; return t; }
    const_iterator& operator+=(difference_type n) { it_ += n; return *this; }
    const_iterator& operator-=(difference_type n) { it_ -= n; return *this; }
    friend const_iterator operator+(const_iterator a, difference_type n) { a += n; return a; }
    friend const_iterator operator+(difference_type n, const_iterator a) { a += n; return a; }
    friend const_iterator operator-(const_iterator a, difference_type n) { a -= n; return a; }
    friend difference_type operator-(const const_iterator& a, const const_iterator& b) { return a.it_ - b.it_; }
    friend bool operator==(const const_iterator& a, const const_iterator& b) { return a.it_ == b.it_; }
    friend bool operator!=(const const_iterator& a, const const_iterator& b) { return a.it_ != b.it_; }
    friend bool operator<(const const_iterator& a, const const_iterator& b) { return a.it_ < b.it_; }
    friend bool operator>(const const_iterator& a, const const_iterator& b) { return a.it_ > b.it_; }
    friend bool operator<=(const const_iterator& a, const const_iterator& b) { return a.it_ <= b.it_; }
    friend bool operator>=(const const_iterator& a, const const_iterator& b) { return a.it_ >= b.it_; }
  };
  using iterator = const_iterator;   // 只读：iterator == const_iterator

  const_iterator begin() const noexcept { return const_iterator{frames_.begin()}; }
  const_iterator end() const noexcept { return const_iterator{frames_.end()}; }
  const_iterator cbegin() const noexcept { return begin(); }
  const_iterator cend() const noexcept { return end(); }

  // ── 显式逃生口：需要真容器时调用（深拷，明确写出来）──
  std::vector<T> materialize() const {
    std::vector<T> out;
    out.reserve(frames_.size());
    for (const auto& sp : frames_) out.push_back(*sp);
    return out;
  }

private:
  std::vector<std::shared_ptr<const T>> frames_;   // 唯一成员，不再暴露
};

} // namespace fins::rt