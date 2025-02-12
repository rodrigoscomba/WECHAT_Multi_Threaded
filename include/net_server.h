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
#include <string>
#include <fstream>
#include <sstream>
#include<random>
#include<vector>
#include <locale> 
#include <codecvt>
#include <cstdlib>
#include <iostream>
#include <filesystem>
#include <string>
#include <set>
#include <limits>
#include <direct.h> // For _getcwd on Windows
#define ASIO_STANDALONE
#include <asio.hpp>
#include <asio/ts/buffer.hpp>
#include <asio/ts/internet.hpp>

#include "net_connectedClient.h"
#include "tsqueue.h"

class net_server
{
public:
	net_server(uint16_t port);
	virtual ~net_server();

public:
	bool Start();
	void StopContextThread();
	void MsgGroup(std::vector<uint8_t>& msg, std::shared_ptr<net_connectedClient> client_ignore);
	void WaitForClientConnectionAndRead();
	void SaveData();
	void LoadData();
protected:

	std::deque<std::shared_ptr<net_connectedClient>> m_deqClients;
	asio::io_context m_context;
	std::thread m_thrContext;
	asio::ip::tcp::acceptor m_acceptor;
	std::unordered_map<std::string, UsersRecord> UsersData; //first field of map, User ID
	uint8_t nGroups = 0;
	std::unordered_map<uint8_t, std::string> GroupIDName;
};

