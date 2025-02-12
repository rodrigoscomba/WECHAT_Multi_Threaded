#include "../include/tsqueue.h"

const std::vector<uint8_t>& tsqueue::front()
{
	std::scoped_lock lock(muxQueue);
	return deqQueue.front();
}

const std::vector<uint8_t>& tsqueue::back()
{
	std::scoped_lock lock(muxQueue);
	return deqQueue.back();
}

void tsqueue::push_front(const std::vector<uint8_t>& item)
{
	std::scoped_lock lock(muxQueue);
	deqQueue.emplace_front(std::move(item));

	std::unique_lock<std::mutex> ul(muxBlocking);
	cvBlocking.notify_one();
}

void tsqueue::push_back(const std::vector<uint8_t>& item)
{
	std::scoped_lock lock(muxQueue);
	deqQueue.emplace_back(std::move(item));

	std::unique_lock<std::mutex> ul(muxBlocking);
	cvBlocking.notify_one();
}

std::vector<uint8_t> tsqueue::pop_front()
{
	std::scoped_lock lock(muxQueue);
	auto t = std::move(deqQueue.front());
	deqQueue.pop_front();
	return t;
}

std::vector<uint8_t> tsqueue::pop_back()
{
	std::scoped_lock lock(muxQueue);
	auto t = std::move(deqQueue.back());
	deqQueue.pop_back();
	return t;
}

bool tsqueue::empty()
{
	std::scoped_lock lock(muxQueue);
	return deqQueue.empty();
}

size_t tsqueue::count()
{
	std::scoped_lock lock(muxQueue);
	return deqQueue.size();
}

void tsqueue::clear()
{
	std::scoped_lock lock(muxQueue);
	deqQueue.clear();
}

void tsqueue::wait()
{
	while (empty())
	{
		std::unique_lock<std::mutex> ul(muxBlocking);
		cvBlocking.wait(ul);
	}
}
