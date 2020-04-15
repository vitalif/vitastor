#pragma once
#include <string>
#include <vector>

int extract_port(std::string & host);

std::vector<std::string> getifaddr_list();
