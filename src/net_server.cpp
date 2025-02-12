#include "../include/net_server.h"
#include "../include/net_connectedClient.h"

std::vector<uint8_t> vBuffer;

std::vector<net_connectedClient*> vClients;
net_server::net_server(uint16_t port)
	: m_acceptor(
		m_context, asio::ip::tcp::endpoint{
			asio::ip::tcp::v4(), port })
{
}

net_server::~net_server()
{
	StopContextThread();
	std::cout << "[SERVER] Stopped!" << std::endl;
}

bool net_server::Start()
{
	LoadData();
	nGroups = GroupIDName.size();

	std::cout << "[SERVER] Started!" << std::endl;
	try
	{
		WaitForClientConnectionAndRead();
		m_thrContext = std::thread([this]() {m_context.run(); });
	}
	catch (std::exception& e)
	{
		std::cerr << "[SERVER] Exception: " << e.what() << std::endl;
		return false;
	}
	return true;
}

void net_server::StopContextThread()
{
	//m_socket.release();
	m_context.stop();
	if (m_thrContext.joinable()) m_thrContext.join();

}

void net_server::MsgGroup(std::vector<uint8_t>& msg, std::shared_ptr<net_connectedClient> client_ignore)
{
	for (auto& client : m_deqClients)
	{
		if (client != client_ignore) {
			client->AddMsgToMsgQueue(msg);
			client->WriteMsg();
		}
	}
}

void net_server::WaitForClientConnectionAndRead()
{
	m_acceptor.async_accept(
		[this](std::error_code ec, asio::ip::tcp::socket socket)
		{
			if (!ec)
			{
				std::cout << "[SERVER] New Connection: " << socket.remote_endpoint() << std::endl;
				std::shared_ptr<net_connectedClient> newClient = std::make_shared<net_connectedClient>(m_context, std::move(socket), 0, m_deqClients, UsersData, nGroups, GroupIDName);
				m_deqClients.push_back(std::move(newClient));
				std::cout << "[SERVER] NEW Client ID: " << (unsigned int)m_deqClients.back()->GetID() << std::endl;
				m_deqClients.back()->ReadHeader();
			}
			else
			{
				std::cout << "[SERVER] New Connection Error: " << ec.message() << std::endl;
			}
			WaitForClientConnectionAndRead();
		}
	);
}
void net_server::LoadData()
{
    std::cout << "Starting LoadData...\n";

    // Dynamically get the path to the Documents folder
    char* userProfile = nullptr;
    size_t len = 0;

    if (_dupenv_s(&userProfile, &len, "USERPROFILE") != 0 || userProfile == nullptr) {
        std::cerr << "Error: Could not retrieve USERPROFILE environment variable.\n";
        return;
    }

    std::filesystem::path documentsPath = std::filesystem::path(userProfile) / "Documents" / "WeChatServer" / "users.csv";
    std::cout << "Users data path: " << documentsPath << "\n";

    // Open the users.csv file
    std::fstream f_users;
    f_users.open(documentsPath, std::ios::in);
    if (!f_users.is_open()) {
        std::cerr << "Error: Could not open the file '" << documentsPath << "'.\n";
        return;
    }

    std::cout << "Loading user data...\n";
    std::string current_line, username, password, ID, fname, gID, gName, discard;
    while (getline(f_users, current_line))
    {
        if (!current_line.empty())
        {
            std::istringstream line{ current_line };
            if (getline(line, username, ','))
                if (getline(line, ID, ','))
                    if (getline(line, password, ',')) {
                        UsersData[username] = UsersRecord{ static_cast<uint8_t>(stoul(ID, 0, 0)), password };
                        std::cout << "Loaded user: " << username << " with ID: " << ID << "\n";
                        getline(line, discard, '\n');
                    }
        }
    }

    f_users.clear();
    f_users.seekg(0);

    std::cout << "Loading friends...\n";
    while (getline(f_users, current_line))
    {
        if (!current_line.empty())
        {
            std::istringstream line{ current_line };
            if (getline(line, username, ','))
                if (getline(line, discard, ','))
                    if (getline(line, discard, ','))
                    {
                        if (getline(line, current_line, '\n'))
                        {
                            std::istringstream line2{ current_line };
                            while (getline(line2, fname, ','))
                            {
                                if (!fname.empty())
                                {
                                    auto it = UsersData.find(fname);
                                    if (it != UsersData.end()) {
                                        UsersData.at(username).Friends.push_back(it);
                                        std::cout << "Added friend: " << fname << " to user: " << username << "\n";
                                    }
                                }
                            }
                        }
                    }
        }
    }
    f_users.close();

    documentsPath = std::filesystem::path(userProfile) / "Documents" / "WeChatServer" / "groups.csv";
    std::cout << "Groups data path: " << documentsPath << "\n";

    // Open the groups.csv file
    std::fstream f_groups;
    f_groups.open(documentsPath, std::ios::in);
    if (!f_groups.is_open()) {
        std::cerr << "Error: Could not open the file '" << documentsPath << "'.\n";
        return;
    }

    std::cout << "Loading group data...\n";
    while (getline(f_groups, current_line))
    {
        if (!current_line.empty())
        {
            std::istringstream line{ current_line };
            if (getline(line, gID, ','))
                if (getline(line, gName, ','))
                {
                    GroupIDName[static_cast<uint8_t>(stoul(gID, 0, 0))] = { gName };
                    std::cout << "Loaded group: " << gName << " with ID: " << gID << "\n";

                    if (getline(line, current_line, '\n'))
                    {
                        std::istringstream line2{ current_line };
                        while (getline(line2, username, ','))
                        {
                            if (!username.empty())
                            {
                                UsersData.at(username).Groups.push_back(static_cast<uint8_t>(stoul(gID, 0, 0)));
                                std::cout << "Added user: " << username << " to group: " << gName << "\n";
                            }
                        }
                    }
                }
        }
    }
    f_groups.close();
    free(userProfile);
    std::cout << "Finished LoadData.\n";
}

void net_server::SaveData()
{
    std::cout << "Starting SaveData...\n";

    char* userProfile = nullptr;
    size_t len = 0;

    if (_dupenv_s(&userProfile, &len, "USERPROFILE") != 0 || userProfile == nullptr) {
        std::cerr << "Error: Could not retrieve USERPROFILE environment variable.\n";
        return;
    }

    std::filesystem::path documentsPath = std::filesystem::path(userProfile) / "Documents" / "WeChatServer";
    std::filesystem::path usersPath = documentsPath / "users.csv";
    std::filesystem::path tempUsersPath = documentsPath / "temp_users.csv";
    std::filesystem::path groupsPath = documentsPath / "groups.csv";
    std::filesystem::path tempGroupsPath = documentsPath / "temp_groups.csv";
    free(userProfile);

    if (!std::filesystem::exists(documentsPath)) {
        std::filesystem::create_directories(documentsPath);
        std::cout << "Created missing directory: " << documentsPath << "\n";
    }

    // Save user data
    std::fstream f_users(tempUsersPath, std::ios::out | std::ios::app);
    if (!f_users.is_open()) {
        std::cerr << "Error: Could not open file '" << tempUsersPath << "' for writing.\n";
        return;
    }

    std::cout << "Saving user data...\n";
    for (auto& it : UsersData) {
        f_users << it.first << "," << (unsigned int)it.second.ID << "," << it.second.Password;
        for (auto& it2 : it.second.Friends) {
            f_users << "," << it2->first;
        }
        f_users << "\n";
        std::cout << "Saved user: " << it.first << " with ID: " << (unsigned int)it.second.ID << "\n";
    }
    f_users.close();
    if (std::filesystem::exists(usersPath)) {
        std::filesystem::remove(usersPath);
    }
    std::filesystem::rename(tempUsersPath, usersPath);

    // Save group data
    std::fstream f_groups(tempGroupsPath, std::ios::out | std::ios::app);
    if (!f_groups.is_open()) {
        std::cerr << "Error: Could not open file '" << tempGroupsPath << "' for writing.\n";
        return;
    }

    std::cout << "Saving group data...\n";
    for (auto& it : GroupIDName) {
        f_groups << (unsigned int)it.first << "," << it.second;
        for (auto& it2 : UsersData) {
            for (auto& it3 : it2.second.Groups) {
                if (it3 == it.first) {
                    f_groups << "," << it2.first;
                }
            }
        }
        f_groups << "\n";
        std::cout << "Saved group: " << it.second << " with ID: " << (unsigned int)it.first << "\n";
    }
    f_groups.close();
    if (std::filesystem::exists(groupsPath)) {
        std::filesystem::remove(groupsPath);
    }
    std::filesystem::rename(tempGroupsPath, groupsPath);

    std::cout << "Finished SaveData.\n";
}
