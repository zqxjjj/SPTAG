// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include <iostream>

#include "inc/Core/Common.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Core/Common/TruthSet.h"

#include "inc/RemoteServing/RemoteServing.h"
 
using namespace SPTAG;

std::shared_ptr<VectorIndex> readIndex(std::map<std::string, std::map<std::string, std::string>>* config_map, const char* configurationPath) {
	Helper::IniReader iniReader;
	VectorValueType valueType;
	DistCalcMethod distCalcMethod;
	iniReader.LoadIniFile(configurationPath);

	const std::string SEC_BASE = "Base";
	const std::string SEC_SELECT_HEAD = "SelectHead";
	const std::string SEC_BUILD_HEAD = "BuildHead";
	const std::string SEC_BUILD_SSD_INDEX = "BuildSSDIndex";
	const std::string SEC_SEARCH_SSD_INDEX = "SearchSSDIndex";

	(*config_map)[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
	(*config_map)[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
	(*config_map)[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
	(*config_map)[SEC_BUILD_SSD_INDEX] = iniReader.GetParameters(SEC_BUILD_SSD_INDEX);

	valueType = iniReader.GetParameter(SEC_BASE, "ValueType", valueType);
	distCalcMethod = iniReader.GetParameter(SEC_BASE, "DistCalcMethod", distCalcMethod);
	bool buildSSD = iniReader.GetParameter(SEC_BUILD_SSD_INDEX, "isExecute", false);

	for (auto& KV : iniReader.GetParameters(SEC_SEARCH_SSD_INDEX)) {
		std::string param = KV.first, value = KV.second;
		if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "BuildSsdIndex")) continue;
		if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "isExecute")) continue;
		if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "PostingPageLimit")) param = "SearchPostingPageLimit";
		if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "InternalResultNum")) param = "SearchInternalResultNum";
		(*config_map)[SEC_BUILD_SSD_INDEX][param] = value;
	}

	std::shared_ptr<VectorIndex> index = VectorIndex::CreateInstance(IndexAlgoType::SPANN, valueType);

	for (auto& sectionKV : *config_map) {
		for (auto& KV : sectionKV.second) {
			index->SetParameter(KV.first, KV.second, sectionKV.first);
		}
	}
	return index;
}

int BootProgramClient(const char* configurationPath) {
	std::map<std::string, std::map<std::string, std::string>> my_map;
	auto index = readIndex(&my_map, configurationPath);
	#define DefineVectorValueType(Name, Type) \
	if (index->GetVectorValueType() == VectorValueType::Name) { \
		RemoteServing::SPectrumSearchClient((SPANN::Index<Type>*)(index.get())); \
	} \

	#include "inc/Core/DefinitionList.h"
	#undef DefineVectorValueType
	return 0;
}

// switch between exe and static library by _$(OutputType) 
#ifdef _exe

int main(int argc, char* argv[]) {
	if (argc < 3)
	{
		LOG(Helper::LogLevel::LL_Error,
			"SPectrum & Distributed SPANN Requires 3 parameters\n");
		exit(-1);
	}

	std::string loadMode(argv[2]);
	int ret;
	if (loadMode.find("client") != std::string::npos) {
		LOG(Helper::LogLevel::LL_Info,
			"Distributed Search Client\n");
		ret = BootProgramClient(argv[1]);
	} else {
		std::string basePath(argv[1]);
		if (loadMode.find("distKV") != std::string::npos) {
			int status = system(("cp "+ basePath + "/indexloader.ini.distkv " + basePath + "/indexloader.ini").c_str());
			if (status == 0) {
				LOG(Helper::LogLevel::LL_Info, "Distributed Search DistKV Config File Copied\n");
			} else {
				LOG(Helper::LogLevel::LL_Info, "Error in Config File Copy, Exit\n");
				exit(0);
			}
		} else if (loadMode.find("topLayer") != std::string::npos) {
			int status = system(("cp "+ basePath + "/indexloader.ini.toplayernode " + basePath + "/indexloader.ini").c_str());
			if (status == 0) {
				LOG(Helper::LogLevel::LL_Info, "Distributed Search TopLayerNode Config File Copied\n");
			} else {
				LOG(Helper::LogLevel::LL_Info, "Error in Config File Copy, Exit\n");
				exit(0);
			}
		} else if (loadMode.find("singleIndex") != std::string::npos) {
			int status = system(("cp "+ basePath + "/indexloader.ini.singleindex " + basePath + "/indexloader.ini").c_str());
			if (status == 0) {
				LOG(Helper::LogLevel::LL_Info, "Distributed Search SingleIndex Config File Copied\n");
			} else {
				LOG(Helper::LogLevel::LL_Info, "Error in Config File Copy, Exit\n");
				exit(0);
			}
		} else {
			LOG(Helper::LogLevel::LL_Info, "Can not identify mode, Exit\n");
			exit(0);
		}
		ret = RemoteServing::BootProgram(argv[1]);
	}
	return ret;
}

#endif
