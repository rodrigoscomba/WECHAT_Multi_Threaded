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
#ifdef __unix__
#include <postgresql/libpq-fe.h>
#else
#include <libpq-fe.h>
#endif

#ifdef _WIN32
#define _WIN32_WINNT 0x0A00
#endif

#ifndef DEV_DEBUG
#define DEV_DEBUG
#endif

#define ASIO_STANDALONE
#include <asio.hpp>
#include <asio/buffer.hpp>
#include <asio/ts/internet.hpp>

#include "tsqueue.h"

const std::string kDbHostIP = "127.0.0.1";        // Use localhost for local PostgreSQL
const std::string kDbName = "wechat_multi";       // The database name you created
const std::string kDbUsername = "postgres";       // Your PostgreSQL username (default: postgres)
const std::string kDbPassword = "patto";  // Replace with the password you set during installation


struct UsersRecord
{
	uint8_t ID;
	std::string Password;
	std::vector<std::unordered_map<std::string, UsersRecord>::iterator> Friends;
	std::vector<uint8_t> Groups;
};

class net_connectedClient : std::enable_shared_from_this<net_connectedClient>
{
public:
	net_connectedClient(asio::io_context& context, asio::ip::tcp::socket socket, uint8_t ID, std::deque<std::shared_ptr<net_connectedClient>>& deqClients, std::unordered_map<std::string, UsersRecord>& UsersData, uint8_t& nGroups, std::unordered_map<uint8_t, std::string>& GroupIDName);

public:

	void MsgGroup(std::vector<uint8_t> msg, uint8_t client_ignore);
	void MsgGroup(uint8_t gid, std::vector<uint8_t> msg, uint8_t client_ignore);
	void MsgFriend(uint8_t fid, std::vector<uint8_t> msg);
	void ReadHeader();
	void ReadBody(uint8_t& type, uint8_t& id, uint8_t& gid, uint8_t& size);
	void AddMsgToMsgQueue(std::vector<uint8_t> msg);
	void WriteMsg();
	void WriteBody(uint8_t& type, uint8_t& id, uint8_t& gid, uint8_t& size);
	uint8_t GetID() const;
	uint8_t GetGroupID() const;
	void SetGroupID(const uint8_t& groupID);
	bool escreve_msg_db(std::string grupo, std::string sender, std::string message);
	int busca_msg(std::string grupo, std::vector<std::vector<std::string>>& vetor);
	void Disconnect();

protected:
	asio::io_context& m_context;
	asio::ip::tcp::socket m_socket;
	uint8_t m_ID = 0;
	uint8_t m_GroupID;
	uint8_t& nGroups;
	std::vector<uint8_t> m_Header;
	std::vector<uint8_t> m_Body;
	std::vector<uint8_t> temp_message;
	tsqueue m_qMessagesOut;
	std::deque<std::shared_ptr<net_connectedClient>>& m_deqClients;
	std::unordered_map<std::string, UsersRecord>& UsersData; //first field of map, User ID
	std::unordered_map<std::string, UsersRecord>::iterator m_it_username;
	std::unordered_map<uint8_t, std::string>& GroupIDName; //Group-IDs and correspondent Group-Names
};

