#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <deque>
#include <optional>
#include <vector>
#include <iostream>
#include <algorithm>
#include <chrono>
#include <cstdint>

class tsqueue
{
public:
	tsqueue() = default;
	tsqueue(const tsqueue&) = delete;
	virtual ~tsqueue() { clear(); }

public:
	const std::vector<uint8_t>& front();
	const std::vector<uint8_t>& back();
	std::vector<uint8_t> pop_front();
	std::vector<uint8_t> pop_back();
	void push_back(const std::vector<uint8_t>& item);
	void push_front(const std::vector<uint8_t>& item);
	bool empty();
	size_t count();
	void clear();
	void wait();

protected:
	std::mutex muxQueue;
	std::deque<std::vector<uint8_t>> deqQueue;
	std::condition_variable cvBlocking;
	std::mutex muxBlocking;
};
