// minitsdb/src/skiplist.h
//
// 手写跳表：MemTable 的底层有序结构（参考 LevelDB，简化）。
// 线程安全说明：本类内部不做同步，由上层（MemTable）负责加锁。
//
// 为什么 MemTable 选用跳表而非 std::map（红黑树）？
//   1) 范围查询友好：跳表本质是多层有序链表，范围扫描只需沿最底层
//      链表顺序前进，代价 O(命中数)；红黑树范围扫描需维护迭代状态。
//   2) 无锁化潜力：插入仅修改局部指针，便于改造为"一写多读"无锁结构
//      （LevelDB 即如此）；红黑树的旋转/重染色难以无锁化。
//   3) 实现直观，便于调试与面试讲解。
#ifndef MINITSDB_SKIPLIST_H
#define MINITSDB_SKIPLIST_H

#include <cassert>
#include <random>

namespace minitsdb {

template <typename Key, typename Value, typename Comparator>
class SkipList {
public:
    // 理论容量 ~ 1 << kMaxLevel 数量级，足够 MemTable 在写满前 flush。
    static constexpr int kMaxLevel = 12;

    // 跳表节点。next_ 为固定大小数组换取实现简洁
    // （LevelDB 用变长数组 new[] 省 memory，面试可作对比点）。
    struct Node {
        Key key;
        Value value;
        Node* next_[kMaxLevel];

        Node(const Key& k, const Value& v) : key(k), value(v) {
            for (int i = 0; i < kMaxLevel; ++i) next_[i] = nullptr;
        }
    };

    SkipList() : compare_(), rnd_(0x9E3779B9u) {
        head_ = new Node(Key{}, Value{});
        max_height_ = 1;
    }

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // 插入或更新：key 相等则覆盖 value（同时间戳重写语义）。
    void Insert(const Key& key, const Value& value) {
        Node* prev[kMaxLevel];
        Node* x = FindGreaterOrEqual(key, prev);
        if (x != nullptr && Equal(key, x->key)) {
            x->value = value;
            return;
        }
        int height = RandomHeight();
        if (height > max_height_) {
            for (int i = max_height_; i < height; ++i) prev[i] = head_;
            max_height_ = height;
        }
        Node* n = new Node(key, value);
        for (int i = 0; i < height; ++i) {
            n->next_[i] = prev[i]->next_[i];
            prev[i]->next_[i] = n;
        }
    }

    bool Get(const Key& key, Value* out) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        if (x != nullptr && Equal(key, x->key)) {
            *out = x->value;
            return true;
        }
        return false;
    }

    bool Contains(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        return x != nullptr && Equal(key, x->key);
    }

    // 返回 >= key 的首个节点（范围扫描的起点）；nullptr 表示超过末尾。
    Node* FindGreaterOrEqual(const Key& key) const {
        return FindGreaterOrEqual(key, nullptr);
    }

    // 底层链表的下一个节点，用于范围扫描。
    Node* NextOf(Node* n) const { return n != nullptr ? n->next_[0] : nullptr; }

    const Key& KeyOf(Node* n) const { return n->key; }
    const Value& ValueOf(Node* n) const { return n->value; }

    int MaxHeight() const { return max_height_; }

    // 节点数（用于测试/调试）
    size_t Count() const;

private:
    bool Equal(const Key& a, const Key& b) const {
        return compare_(a, b) == 0;
    }

    int RandomHeight() {
        static constexpr unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxLevel && (rnd_() % kBranching) == 0) ++height;
        assert(height > 0 && height <= kMaxLevel);
        return height;
    }

    bool KeyIsAfterNode(const Key& key, Node* n) const {
        return n != nullptr && compare_(n->key, key) < 0;
    }

    Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
        Node* x = head_;
        int level = max_height_ - 1;
        while (true) {
            Node* next = x->next_[level];
            if (KeyIsAfterNode(key, next)) {
                x = next;
            } else {
                if (prev != nullptr) prev[level] = x;
                if (level == 0) return next;
                --level;
            }
        }
    }

    int max_height_;
    Node* head_;
    Comparator compare_;
    mutable std::mt19937 rnd_;
};

// 跳表节点计数（沿最底层链表遍历）
template <typename Key, typename Value, typename Comparator>
size_t SkipList<Key, Value, Comparator>::Count() const {
    size_t count = 0;
    Node* n = head_->next_[0];
    while (n != nullptr) {
        ++count;
        n = n->next_[0];
    }
    return count;
}

}  // namespace minitsdb

#endif  // MINITSDB_SKIPLIST_H
