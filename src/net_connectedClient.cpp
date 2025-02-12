#include "../include/net_connectedClient.h"

net_connectedClient::net_connectedClient(asio::io_context& context, asio::ip::tcp::socket socket, uint8_t ID, std::deque<std::shared_ptr<net_connectedClient>>& deqClients, std::unordered_map<std::string, UsersRecord>& UsersData, uint8_t& nGroups, std::unordered_map<uint8_t, std::string>& GroupIDName)
	: m_context(context), m_socket(std::move(socket)), m_ID(ID), m_Header(std::vector<uint8_t>(4)), m_Body(std::vector<uint8_t>(500)), m_deqClients(deqClients), UsersData(UsersData), nGroups(nGroups), GroupIDName(GroupIDName)
{
}

void net_connectedClient::ReadHeader()
{

	asio::async_read(m_socket, asio::buffer(m_Header.data(), sizeof(uint32_t)),
		[this](std::error_code ec, std::size_t length)
		{
			if (!ec)
			{
				uint8_t type = m_Header[0];
				uint8_t ID = m_Header[1];
				uint8_t GID = m_Header[2];
				uint8_t body_size = m_Header[3];
				std::cout << "Received Message:\n";
				std::cout << "type = " << (unsigned int)type << "\n";
				std::cout << "ID = " << (unsigned int)ID << "\n";
				std::cout << "GID = " << (unsigned int)GID << "\n";
				std::cout << "body_size = " << (unsigned int)body_size << "\n";


				if (body_size > 0)
				{
					ReadBody(type, ID, GID, body_size);
				}
				else
				{
					if (type == 'E' && ID == m_ID && m_ID != 0)
					{
						std::vector<uint8_t> ack = { type, ID , GID, 0 };
						SetGroupID(GID);
						AddMsgToMsgQueue(ack);
						if (GID == 0)
							std::cout << "[SERVER]" << m_it_username->first << " left group \"" << GroupIDName[GID] << "\"\n";
						else
							std::cout << "[SERVER]" << m_it_username->first << " entered group \"" << GroupIDName[GID] << "\"\n";

						std::vector<std::vector<std::string>> v_msgs_SQL;
						std::string msg_SQL;
						int n_msg_SQL = busca_msg(GroupIDName.at(GID), v_msgs_SQL);
						if (n_msg_SQL > 0)
						{
#ifdef DEV_DEBUG
							std::cout << "Got " << n_msg_SQL << " msgs from SQL\n";
#endif // DEV_DEBUG

							for (auto& it : v_msgs_SQL)
							{
								if (it.empty())
									continue;
								if (it.back().empty())
									continue;
								std::string msg = it.back();
								it.pop_back();
								

								if (it.back().empty())
									continue;
								//time not in use for now
								std::string time = it.back();
								it.pop_back();
								
								if (it.back().empty())
									continue;

								std::string sender_name = it.back();
								it.pop_back();
								
								if (UsersData.find(sender_name) == UsersData.end())
								{
									continue;
								}

								std::string group = it.back();
								it.pop_back();
#ifdef DEV_DEBUG
								std::cout << "message: " << msg << "\n";
								std::cout << "time: " << time << "\n";
								std::cout << "sender_name: " << sender_name << "\n";
								std::cout << "group: " << group << "\n";
#endif // DEV_DEBUG
								std::vector<uint8_t> msg_to_send = { 'S', UsersData.at(sender_name).ID, GID , (uint8_t)msg.size() };
								std::vector<uint8_t> v_msg(msg.begin(), msg.end());
								msg_to_send.insert(msg_to_send.end(), v_msg.begin(), v_msg.end());
								AddMsgToMsgQueue(msg_to_send);
							}
						}
					}
					else if (type == 'F' && ID == m_ID && m_ID != 0) //Update Client's Friends
					{
						bool has_friends = 0;
						for (auto& it : m_it_username->second.Friends) //go through every friend
						{
							uint8_t group_count = 0;
							std::vector<uint8_t> update_fg;
							std::vector<uint8_t> groups;
							update_fg.push_back(type);
							update_fg.push_back(it->second.ID);

							for (auto& it2 : it->second.Groups) //go through every group of the friend
							{
								std::string no_groupname_for_now;
								std::vector<uint8_t> group_name;
								for (auto& it3 : m_it_username->second.Groups) //go through every group of the user
								{
									if (it3 == it2)
									{
										group_count++; //compare the group and add it to message
										groups.push_back(it3);
									}
								}
							}
							update_fg.push_back(group_count);
							std::vector<uint8_t> name(it->first.begin(), it->first.end());
							if (group_count > 0)
								update_fg.push_back(group_count + (uint8_t)name.size() + 1);
							else
								update_fg.push_back((uint8_t)name.size());
							update_fg.insert(update_fg.end(), name.begin(), name.end());
							if (group_count > 0)
							{
								update_fg.push_back(0);
								update_fg.insert(update_fg.end(), groups.begin(), groups.end());
							}
							AddMsgToMsgQueue(update_fg);
							has_friends = 1;
						}
						std::vector<uint8_t> update_fg = { 'F', 0, 0, 0 }; //signal no more friends
						AddMsgToMsgQueue(update_fg);
						std::cout << "[SERVER, SEND_F_UPDATE] " << m_it_username->first << " -> ID:" << (unsigned int)m_it_username->second.ID << "\n";
					}
					else if (type == 'G' && ID == m_ID && m_ID != 0) //Update Client's Group-Names and Group-IDs
					{
						if(!GroupIDName.empty())
						for (auto& it : m_it_username->second.Groups)
						{
							std::string& group_name = GroupIDName.at(it);
							std::vector<uint8_t> update_g = { type,ID,it,(uint8_t)group_name.size()};
							update_g.insert(update_g.end(), group_name.begin(), group_name.end());
							AddMsgToMsgQueue(update_g);
						}
						std::vector<uint8_t> update_g = { 'G', 0, 0, 0 }; //signal no more groups
						AddMsgToMsgQueue(update_g);
						std::cout << "[SERVER, SEND_G_UPDATE] " << m_it_username->first << " -> ID:" << (unsigned int)m_it_username->second.ID << "\n";
					}
					ReadHeader();
				}
			}
			else
			{
				Disconnect();
				std::cout << "[CLIENT] Async Read Error: " << ec.message() << " --> Assuming Client Disconnected\n";
			}
		});
}
void net_connectedClient::ReadBody(uint8_t& type, uint8_t& id, uint8_t& gid, uint8_t& size)
{
	m_Body.resize(0);
	m_Body.resize(500);
	asio::async_read(m_socket, asio::buffer(m_Body.data(), size),
		[this, type, id, gid, size](std::error_code ec, std::size_t length)
		{
			std::cout << "Message: ";
			for (int i = 0; i < m_Body.size(); i++)
			{
				std::cout << m_Body[i];
			}
			std::cout << "\n";
			if (!ec) {
				//std::cout << "type: " << (unsigned int)type;
				if ((type == 'L' || type == 'U') && m_ID == 0) //Login (L) or Create User Account (U)
				{
					bool next = 0;
					std::string username, password;
					for (int i = 0; i < size; i++)
					{
						if (m_Body[i] == '&')
						{
							next = 1;
							i++;
						}
						if (next == 0) {
							username.push_back(m_Body[i]);
						}
						else
						{
							password.push_back(m_Body[i]);
						}
					}
					if (type == 'L') // if "Login" message, check username and password match
					{
						uint8_t new_id;
						std::vector<uint8_t> ack;
						std::unordered_map<std::string, UsersRecord>::iterator it = UsersData.find(username);
						if (it != UsersData.end())
						{
							if (UsersData.at(username).Password == password)
							{
								m_it_username = it;
								new_id = it->second.ID;
								int i = 0;
								for (auto& client : m_deqClients)
								{
									if (client->GetID() == new_id)
									{
										client->Disconnect();
										//client.reset();// TO BE IMPLEMENTED --------------------------DELETION OF OLD CONNECTED CLIENT FROM DEQUE
										//m_deqClients.erase(
									}
								}
								
								m_ID = new_id;
								
								ack = { type, m_ID, 0, 0 };
								std::cout << "[SERVER, LOGIN] " << username << " -> ID:" << (unsigned int)m_ID << "\n\n";
							}
							else
								ack = { type, 0, 0, 0 }; //case wrong password
						}
						else
							ack = { type, 0, 0, 100 }; //case no user with that name

						AddMsgToMsgQueue(ack);
					}
					else if (type == 'U') //if "Create User Account" check for no same username and add username and password
					{
						std::vector<uint8_t> ack;
						if (UsersData.find(username) != UsersData.end())
						{
							ack = { type, 0, 0, 0 };
						}
						else
						{
							UsersRecord& new_user = UsersData[username];
							new_user.ID = UsersData.size();
							new_user.Password = password;
							ack = { type, 100 , 0, 0 };
						}
						AddMsgToMsgQueue(ack);
					}
				}
				else if (type == 'A' && m_ID > 0)
				{
					std::string friends_username;
					for (int i = 0; i < size; i++)
					{
						friends_username.push_back(m_Body[i]);
					}
					std::unordered_map<std::string, UsersRecord>::iterator it = UsersData.find(friends_username);
					if (it != UsersData.end())
					{
						UsersData.at(m_it_username->first).Friends.push_back(it);
						UsersData.at(friends_username).Friends.push_back(m_it_username); //-------------------------------------------------later, add option for friend to accept request
						uint8_t fid = UsersData.at(friends_username).ID;

						std::cout << "[SERVER, ADD FRIENDS] \"" << m_it_username->first << " ID:" << (unsigned int)m_it_username->second.ID
							<< "\" added \"" << friends_username << " ID:" << (unsigned int)fid << "\"\n";

						std::vector<uint8_t> ack = { 'A', fid, 0, 0 };
						std::vector<uint8_t> v_friend_username(m_it_username->first.begin(), m_it_username->first.end());
						std::vector<uint8_t> friend_msg = { 'F', m_it_username->second.ID, 0, (uint8_t)m_it_username->first.size()};
						friend_msg.insert(friend_msg.end(), v_friend_username.begin(), v_friend_username.end());
						AddMsgToMsgQueue(ack);
						MsgFriend(fid, friend_msg);
					}
					else
					{
						std::vector<uint8_t> ack = { 'A', 0, 0, 0 };
						AddMsgToMsgQueue(ack);
					}
				}
				else if (type == 'S' && m_ID != 0)
				{
					std::vector<uint8_t> mensagem;
					mensagem.push_back('S');
					mensagem.push_back(id);
					mensagem.push_back(gid);
					mensagem.push_back(size);
					for (int i = 0; i < size; i++)
					{
						mensagem.push_back(m_Body[i]);
					} 
					MsgGroup(mensagem, id);
					std::string msg_to_db(mensagem.begin()+4, mensagem.end());
					if (escreve_msg_db(GroupIDName.at(gid), m_it_username->first, msg_to_db))
					{
#ifdef DEV_DEBUG
						std::cout << "WROTE MESSAGE TO DB\n";
#endif // DEV_DEBUG
					}
#ifdef DEV_DEBUG
					std::cout << "MENSAGEM A ENVIAR: ";
					for (int i = 0; i < size + 4; i++)
					{
						std::cout << mensagem[i];
					}
					std::cout << "\n";
#endif // DEV_DEBUG
				}
				else if (type == 'C' && m_ID != 0) //CREATE GROUP AND/OR ADD FRIENDS TO GROUP
				{
					std::vector<uint8_t> refused_IDs;
					std::vector<uint8_t> ack;
					std::vector<uint8_t> group_name;
					std::vector<uint8_t> group_msg;
					uint8_t group_id = nGroups;

					bool done_with_gname = 0;
					if (gid == 0) //create group (=0); add people to group (>0)
					{
						nGroups++;
						group_id = nGroups;
						m_it_username->second.Groups.push_back(group_id);
					}
					for (int i = 0; i < size; i++) //check if all IDs to be added to group are friends
					{
						if(done_with_gname)
						for (auto& ti : m_it_username->second.Friends)
						{
							if (ti->second.ID == m_Body[i])
							{
								ti->second.Groups.push_back(group_id);
								break;
							}
							if (ti == m_it_username->second.Friends.back())
							{
								refused_IDs.push_back(ti->second.ID);
							}
						}
						else if (m_Body[i] != 0 && !done_with_gname)
						{
							group_name.push_back(m_Body[i]);
							continue;
						}
						else
						{
							done_with_gname = 1;
							continue;
						}
					}
					if (!refused_IDs.empty()) //IDs refused because not friends, //not yet implemented on client side
					{
						ack = { type, group_id , 0, (uint8_t)refused_IDs.size() };
						ack.insert(ack.end(), refused_IDs.begin(), refused_IDs.end());
					}
					else {
						ack = { type, group_id , 0, 0 };
					}
					if (gid == 0)
					{
						std::string group(group_name.begin(), group_name.end());
						GroupIDName[group_id] = {group};
						group_msg = { 'G', m_ID, group_id, (uint8_t)group_name.size()};
						group_msg.insert(group_msg.end(), group_name.begin(), group_name.end());

						std::cout << "[SERVER, G_CREATE] \"" << m_it_username->first << " ID:" << (unsigned)m_ID << "\" created group " << GroupIDName.at(group_id) << " ID:" << (unsigned int)group_id << "\n";
					}
					else if (gid > 0) //not implemented yet on client side, and needs safe-guard for group ID not existing---------------------------------------------------------//
						std::cout << "[SERVER, G_ADD FRIENDS] \"" << m_it_username->first << " ID:" << (unsigned)m_ID << "\" added friends to group ID:" << (unsigned int)group_id << "\n";
					AddMsgToMsgQueue(ack);
					
					MsgGroup(group_id, group_msg, m_ID);
				}
			}
			else
			{
				Disconnect();
				std::cout << "[CLIENT] Async Read Error: " << ec.message() << " --> Assuming Client Disconnected\n";
			}
			ReadHeader();
		});
}
void net_connectedClient::AddMsgToMsgQueue(std::vector<uint8_t> msg)
{
	bool writeagain = 0;
	if (m_qMessagesOut.empty()) {
		writeagain = 1;
	}
	m_qMessagesOut.push_back(std::move(msg));


	if (writeagain)
	{
		WriteMsg();
	}
}
void net_connectedClient::MsgGroup(std::vector<uint8_t> msg, uint8_t client_ignore)
{
	for (int i = 0; i < m_deqClients.size(); i++) {
		if (m_deqClients[i]->GetID() != client_ignore && m_deqClients[i]->GetID() != 0 && m_deqClients[i]->GetGroupID() == m_GroupID) {
			m_deqClients[i]->AddMsgToMsgQueue(msg);
#ifdef DEV_DEBUG
			std::cout << std::endl << "ADDED MESSAGE TO CLIENT " << (unsigned int)m_deqClients[i]->GetID() << "'s QUEUE" << std::endl;
#endif // DEV_DEBUG

		}
	}
}
void net_connectedClient::MsgGroup(uint8_t gid, std::vector<uint8_t> msg, uint8_t client_ignore)
{
	for (int i = 0; i < m_deqClients.size(); i++) {
		if (m_deqClients[i]->GetID() != client_ignore && m_deqClients[i]->GetID() != 0) {
			for (auto& it : m_deqClients[i]->m_it_username->second.Groups)
			{
				if (it == gid)
				{
					m_deqClients[i]->AddMsgToMsgQueue(msg);
#ifdef DEV_DEBUG
					std::cout << std::endl << "ADDED MESSAGE - TYPE " << (unsigned int)msg[0] << " TO CLIENT " << (unsigned int)m_deqClients[i]->GetID() << "'s QUEUE" << std::endl;
#endif // DEV_DEBUG
				}
			}
		}
	}
}
void net_connectedClient::MsgFriend(uint8_t fid, std::vector<uint8_t> msg)
{
	for (int i = 0; i < m_deqClients.size(); i++) {
		if (m_deqClients[i]->GetID() == fid)
		{
			m_deqClients[i]->AddMsgToMsgQueue(msg);
#ifdef DEV_DEBUG
			std::cout << std::endl << "ADDED MESSAGE TO CLIENT " << (unsigned int)m_deqClients[i]->GetID() << "'s QUEUE" << std::endl;
#endif // DEV_DEBUG
		}
	}
}
void net_connectedClient::WriteMsg()

{	if(m_socket.is_open())
	asio::async_write(m_socket, asio::buffer(m_qMessagesOut.front(), sizeof(uint32_t)),
		[this](std::error_code ec, std::size_t length)
		{
			if (!ec)
			{
				temp_message.erase(temp_message.begin(), temp_message.end());
				temp_message.resize(0);
				temp_message.resize(500);
				temp_message = m_qMessagesOut.front();
				m_qMessagesOut.pop_front();

				std::cout << "Sending Message:\n";
				for (int i = 0; i < temp_message.size(); i++)
				{
					std::cout << (unsigned int)temp_message[i] << " ";
				}
				std::cout << "\n";


			}

			if (temp_message[3] > 0) {
				WriteBody(temp_message[0], temp_message[1], temp_message[2], temp_message[3]);
			}
			else if (!m_qMessagesOut.empty())
			{
				WriteMsg();
			}
		});
}
void net_connectedClient::WriteBody(uint8_t& type, uint8_t& id, uint8_t& gid, uint8_t& size)
{
	temp_message.erase(temp_message.begin(), temp_message.begin() + 4);

	asio::async_write(m_socket, asio::buffer(temp_message, size),
		[this, type, id, gid, size](std::error_code ec, std::size_t length)
		{
			if (!ec)
			{
				//nothing
			}

			if (!m_qMessagesOut.empty())
			{
				if (m_socket.is_open())
				WriteMsg();
			}
		});
}
uint8_t net_connectedClient::GetID() const
{
	return m_ID;
}
uint8_t net_connectedClient::GetGroupID() const
{
	return m_GroupID;
}
void net_connectedClient::SetGroupID(const uint8_t& groupID)
{
	m_GroupID = groupID;
}
void net_connectedClient::Disconnect()
{
	if(m_socket.is_open())
		m_socket.close();
}
bool net_connectedClient::escreve_msg_db(std::string group_name, std::string sender_name, std::string message) {
	// Database
	//Tirar a sent_time
	int count_try = 0;
	int retorno;
	time_t curr_time;
	std::string min;
	std::string minaux;
	std::string sent_time;
	std::string hour;
	RETRY :
	tm tm_local;  // Use a local tm structure instead of a pointer
	localtime_s(&tm_local, &curr_time);  // Replace localtime with localtime_s
	//std::cout << "Current local time : " << tm_local.tm_hour << ":" << tm_local.tm_min << ":" << tm_local.tm_sec << "\n";
	hour = std::to_string(tm_local.tm_hour);
	if (tm_local.tm_min < 10) {
		minaux = std::to_string(tm_local.tm_min);
		min = "0" + minaux;
		//std::cout << min << std::endl;
	}
	else {
		min = std::to_string(tm_local.tm_min);
		//std::cout << "MINUTO : " << min << std::endl;
	}
	sent_time = hour + ":" + min;

	std::string dbconn_str = "dbname=" + kDbName + " host=" + kDbHostIP +
		" user=" + kDbUsername + " password=" + kDbPassword +
		" connect_timeout=2";   // not recommended timeout less than 2s

	PGconn* dbconn = PQconnectdb(dbconn_str.c_str());
	if (PQstatus(dbconn) != CONNECTION_OK) {
		std::cerr << "Error when connecting to the database " + dbconn_str
			<< std::endl;

		PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
		dbconn = nullptr;
		count_try++;
		if (count_try < 3)
			goto RETRY;
		else
			return 0;
	}

	// Set psw schema
	//std::cout << "Setting schema to psw..." << std::endl;
	if (PQstatus(dbconn) == CONNECTION_OK) {
		PGresult* result = PQexec(dbconn, "SET search_path TO wechat_multi;");

		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'SET search_path TO psw;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;

			PQclear(result);

			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;

			return 0;
		}
		else {
			PQclear(result);
		}
	}

	// Confirm if the schema was changed
	if (PQstatus(dbconn) == CONNECTION_OK) {
		PGresult* result = PQexec(dbconn, "SHOW search_path;");

		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'SHOW search_path;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;

			PQclear(result);

			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;

			return 0;
		}
		else {
			std::string search_path = PQgetvalue(result, 0, 0);
			PQclear(result);
#ifdef DEV_DEBUG
			std::cout << "Search path: " << search_path << std::endl;
#endif // DEV_DEBUG
		}
	}

	// - WRITE mensagem in the database
	if (PQstatus(dbconn) == CONNECTION_OK) {
		//SELECT * FROM table LIMIT 10 OFFSET N-10


		std::string comando = "INSERT INTO group_messages (group_name,sender_name,sent_time,message_content) VALUES ( '" + group_name + "'" + ", " + "'" + sender_name + "'" + ", " + "'" + sent_time + "'" + ", " + "'" + message + "'" + "); ";
		PGresult* result = PQexec(dbconn, comando.c_str());
		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'INSERT INTO message_content;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;
			PQclear(result);
			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;
			return 0;
		}
		else {
			std::string comando = "SELECT * FROM group_messages WHERE group_name='" + group_name + "' ORDER BY ID DESC LIMIT 1;"; //' LIMIT 10 OFFSET N-10 ||ORDER BY ID DESC LIMIT 40

			PGresult* result = PQexec(dbconn, comando.c_str());
			if (PQgetvalue(result, 0, 2) == sender_name && PQgetvalue(result, 0, 4) == message)

				retorno = 1;

			else {

				retorno = 0;
			}
		}
		PQclear(result);

	}
	PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
	dbconn = nullptr;
	return retorno;
}
int net_connectedClient::busca_msg(std::string group_name, std::vector<std::vector<std::string>>& vetor) {
	// Database
	int tamanho = 0;
	int count_try = 0;
	// HANDLE hConsole = GetStdHandle(STD_OUTPUT_HANDLE);
	std::string anterior;
	std::string dbconn_str = "dbname=" + kDbName + " host=" + kDbHostIP +
		" user=" + kDbUsername + " password=" + kDbPassword +
		" connect_timeout=2";   // not recommended timeout less than 2s
RETRY:
	PGconn* dbconn = PQconnectdb(dbconn_str.c_str());
	if (PQstatus(dbconn) != CONNECTION_OK) {
		std::cerr << "Error when connecting to the database " + dbconn_str
			<< std::endl;

		PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
		dbconn = nullptr;
		if (count_try < 3)
		{
			count_try++;
			goto RETRY;
		}	
		else
			return 0;
	}
	// Set psw schema

	if (PQstatus(dbconn) == CONNECTION_OK) {
		PGresult* result = PQexec(dbconn, "SET search_path TO wechat_multi;");

		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'SET search_path TO psw;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;

			PQclear(result);

			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;

			return -1;
		}
		else {
			PQclear(result);
		}
	}
	//define o gerador random de numeros
	std::random_device rd; // obtain a random number from hardware
	std::mt19937 gen(rd()); // seed the generator
	std::uniform_int_distribution<> distr(30, 37); // define the range
	// Confirm if the schema was changed
	if (PQstatus(dbconn) == CONNECTION_OK) {
		PGresult* result = PQexec(dbconn, "SHOW search_path;");

		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'SHOW search_path;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;

			PQclear(result);

			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;

			return -1;
		}
		else {
			std::string search_path = PQgetvalue(result, 0, 0);
			PQclear(result);

			// std::cout << "Search path: " << search_path << std::endl;
		}
	}

	 // - get all message_content in the database
	if (PQstatus(dbconn) == CONNECTION_OK) {
		//SELECT * FROM table LIMIT 10 OFFSET N-10

		std::string comando = "SELECT * FROM group_messages WHERE group_name='" + group_name + "' ORDER BY ID DESC LIMIT 40;"; //' LIMIT 10 OFFSET N-10 ||ORDER BY ID DESC LIMIT 40

		PGresult* result = PQexec(dbconn, comando.c_str());

		if (PQresultStatus(result) == PGRES_FATAL_ERROR) {
			std::cerr << "[QUERY] Error occurred when executing "
				"'SELECT * FROM message_content;'\n" +
				std::string(PQerrorMessage(dbconn))
				<< std::endl;

			PQclear(result);

			PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
			dbconn = nullptr;

			return -1;
		}
		else {
			//std::cout << std::endl << "All mensagem saved in the database:" << std::endl;

			tamanho = PQntuples(result);

			// for (int i = 0; i < PQntuples(result); i++)
			for (int i = PQntuples(result); i > 0; i--)
			{
				// Vector to store column elements
				std::vector<std::string> v1;

				for (int j = 0; j < 5; j++) {
					v1.push_back(PQgetvalue(result, i - 1, j));

				}
				// Pushing back above 1D vector
				// to create the 2D vector
				vetor.push_back(v1);
			}
			PQclear(result);
		}
	}
	PQfinish(dbconn);  // even if fail, free allocated memory of PGconn object
	dbconn = nullptr;
	return tamanho;
}