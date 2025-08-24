//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {
/**
 *
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */

LRUKNode::LRUKNode() = default;

LRUKNode::LRUKNode(const frame_id_t fid, const size_t k) : fid(fid), k_(k) {
}

LRUKReplacer::LRUKReplacer(const size_t num_frames, const size_t k) : replacer_size_(num_frames), k_(k) {
}

/**
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  if (fifo_list_.size() > 0) {
    auto iter = fifo_list_.rbegin();
    for (; iter != fifo_list_.rend(); iter++) {
      if ((*iter)->is_evictable) {
        break;
      }
    }
    if (iter != fifo_list_.rend()) {
      auto fid = (*iter)->fid;
      node_store_.erase(fid);
      fifo_list_.erase(std::next(iter).base());
      curr_size_--;
      return fid;
    }
    return std::nullopt;
  }
  if (lru_list_.size() > 0) {
    auto iter = lru_list_.rbegin();
    for (; iter != lru_list_.rend(); iter++) {
      if ((*iter)->is_evictable) {
        break;
      }
    }
    if (iter != lru_list_.rend()) {
      auto fid = (*iter)->fid;
      node_store_.erase(fid);
      lru_list_.erase(std::next(iter).base());
      curr_size_--;
      return fid;
    }
  }
  return std::nullopt;
}

/**
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is larger than replacer_size_");
  std::lock_guard lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(frame_id, k_));
    fifo_list_.push_front(&node_store_[frame_id]);
    fid_to_fifo_iter_[frame_id] = fifo_list_.begin();
  }
  auto &node = node_store_[frame_id];
  node.history.push_front(current_timestamp_);
  if (node.history.size() > k_) {
    while (node.history.size() > k_) {
      node.history.pop_back();
    }
    auto iter = fid_to_lru_iter_[frame_id];
    lru_list_.erase(iter);
    auto next_iter = lru_list_.begin();
    for (; next_iter != lru_list_.end(); next_iter++) {
      if ((*next_iter)->history.back() < node.history.back()) {
        break;
      }
    }
    auto new_iter = lru_list_.insert(next_iter, &node);
    fid_to_lru_iter_[frame_id] = new_iter;
  } else if (node.history.size() == k_) {
    const auto iter = fid_to_fifo_iter_[frame_id];
    fifo_list_.erase(iter);
    lru_list_.emplace_front(&node);
    fid_to_lru_iter_[frame_id] = lru_list_.begin();
  }
  current_timestamp_++;
}

/**
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_.at(frame_id);
  if (node.is_evictable && !set_evictable) {
    curr_size_--;
  } else if (!node.is_evictable && set_evictable) {
    curr_size_++;
  }
  node.is_evictable = set_evictable;
}

/**
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (!node.is_evictable) {
    return;
  }
  node_store_.erase(frame_id);
  if (fid_to_fifo_iter_.find(frame_id) != fid_to_fifo_iter_.end()) {
    auto iter = fid_to_fifo_iter_[frame_id];
    fifo_list_.erase(iter);
    fid_to_fifo_iter_.erase(frame_id);
  }
  if (fid_to_lru_iter_.find(frame_id) != fid_to_lru_iter_.end()) {
    auto iter = fid_to_lru_iter_[frame_id];
    lru_list_.erase(iter);
    fid_to_lru_iter_.erase(frame_id);
  }
  curr_size_--;
}

/**
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }
} // namespace bustub