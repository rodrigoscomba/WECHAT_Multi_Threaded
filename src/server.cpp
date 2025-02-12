
#include "../include/net_server.h"

bool FindServerPort(int& port) {
    char* userProfile = nullptr;
    size_t len = 0;

    // Use _dupenv_s to safely get the USERPROFILE environment variable
    if (_dupenv_s(&userProfile, &len, "USERPROFILE") != 0 || userProfile == nullptr) {
        std::cerr << "Error: Could not retrieve USERPROFILE environment variable.\n";
        return false;
    }

    // Build the full path to ServerPort.txt
    std::filesystem::path documentsPath = std::filesystem::path(userProfile) / "Documents" / "WeChatServer" / "ServerPort.txt";

    free(userProfile); // Free the allocated memory

    // Log the constructed path for debugging
    std::cout << "Looking for ServerPort.txt at: " << documentsPath << "\n";

    // Check if the file exists before attempting to open
    if (!std::filesystem::exists(documentsPath)) {
        std::cerr << "Error: File does not exist at '" << documentsPath << "'.\n";
        return false;
    }

    // Open the file
    std::fstream serverdata(documentsPath, std::ios::in);
    if (!serverdata.is_open()) {
        std::cerr << "Error: Could not open the file '" << documentsPath << "'.\n";
        return false;
    }

    // Read the port from the file
    std::string server_port;
    if (getline(serverdata, server_port)) {
        try {
            // Convert the string to an integer
            port = std::stoi(server_port);

            // Validate the port range (0-65535)
            if (port < 0 || port > 65535) {
                throw std::out_of_range("Port number out of valid range (0-65535)");
            }

            serverdata.close();
            std::cout << "Server port successfully retrieved: " << port << "\n";
            return true;
        }
        catch (const std::invalid_argument&) {
            std::cerr << "Error: Invalid port number format in '" << documentsPath << "'.\n";
        }
        catch (const std::out_of_range& e) {
            std::cerr << "Error: Port number out of range in '" << documentsPath << "'. " << e.what() << "\n";
        }
    }
    else {
        std::cerr << "Error: Failed to read the port from '" << documentsPath << "'.\n";
    }
    serverdata.close();
    return false;
}

int main()
{
	std::cout << "LOADING...\n";
	int port;
	if (FindServerPort(port) == 0)
	{
		std::cout << "Couldn't read Server's Port from Documents/WeChatServer/ServerPort.txt\n";
	}
    std::cout << "CCCCCC\n";
	net_server server(port);
    std::cout << "HERE\n";
	server.Start();
	std::string input;
	while (true) {
		std::cin >> input;
		if (input == "/stop")
		{
			server.StopContextThread();
			server.SaveData();
			return 0;
		}
		if (input == "/save")
		{
			server.SaveData();
		}
	}
	return 0;
}