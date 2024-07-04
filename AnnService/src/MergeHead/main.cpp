#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/Utils.h"
#include "inc/Helper/ArgumentsParser.h"
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <future>

#include <iostream>
#include <fstream>
#include <memory>
#include <ratio>
#include <string>
#include <sys/types.h>
#include <unordered_set>

using namespace SPTAG;


class MergeOptions : public Helper::ArgumentsParser
{
public:
    MergeOptions()
    {
        AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
        AddRequiredOption(m_dim, "-d", "--dimension", "Dimension of vector.");
        AddRequiredOption(m_numFiles, "-f", "--numfiles", "Merge from f files");
        AddRequiredOption(m_numOutputFiles, "-n", "--numoutputfiles", "Merge to n files");
        AddRequiredOption(inputDirPrefix, "-idp", "--inputdirprefix", "the prefix of input index dir");
        AddRequiredOption(outputNewDataPath, "-odp", "--outputnewdatapath", "output head data & metadata path");
        
    }

    ~MergeOptions() {}

    SPTAG::VectorValueType m_inputValueType;
    DimensionType m_dim;
    int m_numFiles;
    int m_numOutputFiles;
    std::string inputDirPrefix;
    std::string outputNewDataPath;
} options;

void SetupOutputPtr(int writeFileNo, std::shared_ptr<Helper::DiskIO> optr_data, std::shared_ptr<Helper::DiskIO> optr_meta, std::shared_ptr<Helper::DiskIO> optr_metaindex, int toWriteSize) {

    uint64_t begin = 0;
    std::string outputDataFile = options.outputNewDataPath + FolderSep + "vectors.bin." + std::to_string(writeFileNo);
    std::string outputMetaDataFile = options.outputNewDataPath + FolderSep + "meta.bin." + std::to_string(writeFileNo);
    std::string outputMetaIndexFile = options.outputNewDataPath + FolderSep + "metaIndex.bin." + std::to_string(writeFileNo);

    if (optr_data == nullptr || !optr_data->Initialize(outputDataFile.c_str(), std::ios::out | std::ios::binary)) {
        LOG(Helper::LogLevel::LL_Error, "Failed open output file: %s\n", outputDataFile.c_str());
        exit(1);
    }

    if (optr_meta == nullptr || !optr_meta->Initialize(outputMetaDataFile.c_str(), std::ios::out | std::ios::binary)) {
        LOG(Helper::LogLevel::LL_Error, "Failed open output meta file: %s\n", outputMetaDataFile.c_str());
        exit(1);
    }

    if (optr_metaindex == nullptr || !optr_metaindex->Initialize(outputMetaIndexFile.c_str(), std::ios::out | std::ios::binary)) {
        LOG(Helper::LogLevel::LL_Error, "Failed open output metaIndex file: %s\n", outputMetaIndexFile.c_str());
        exit(1);
    }

    if (optr_data->WriteBinary(4, reinterpret_cast<char*>(&toWriteSize)) != 4) {
        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
        exit(1);
    }
    if (optr_data->WriteBinary(4, reinterpret_cast<char*>(&options.m_dim)) != 4) {
        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
        exit(1);
    }
    if (optr_metaindex->WriteBinary(4, reinterpret_cast<char*>(&toWriteSize)) != 4) {
        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
        exit(1);
    }
    if (optr_metaindex->WriteBinary(sizeof(uint64_t), reinterpret_cast<char*>(&begin)) != sizeof(uint64_t)) {
        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
        exit(1);
    }
}

template <typename ValueType>
void MergeHeadSingleNode() {

    std::uint64_t totalSize = 0;
    std::unordered_set<std::uint64_t> dedupSet;
    std::vector<std::shared_ptr<std::uint64_t>> m_vectorTranslateMaps(options.m_numFiles);

    for (int FileNo = 0; FileNo < options.m_numFiles; FileNo++) {
        // Loading all head translation map
        std::string headDataFile = options.inputDirPrefix + std::to_string(FileNo) + FolderSep + "HeadIndex" + FolderSep + "vectors.bin";
        std::string headMapFile = options.inputDirPrefix + std::to_string(FileNo) + FolderSep + "SPTAGHeadVectorIDs.bin";
        std::string headMetaFile = options.inputDirPrefix + std::to_string(FileNo) + FolderSep + "meta.bin";
        std::string headMetaIndexFile = options.inputDirPrefix + std::to_string(FileNo) + FolderSep + "metaIndex.bin";

        // Get the head vector num
        auto ptr = SPTAG::f_createIO();
        if (ptr == nullptr || !ptr->Initialize(headDataFile.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Cannot Load Head Index Vectors.bin\n");
            exit(0);
        }
        int vec_num;
        if (ptr->ReadBinary(sizeof(int), (char*)&vec_num) != sizeof(int)) {
            LOG(Helper::LogLevel::LL_Error, "Cannot Read vector num from Head Index Vectors.bin\n");
            exit(0);
        }

        m_vectorTranslateMaps[FileNo].reset(new std::uint64_t[vec_num], std::default_delete<std::uint64_t[]>());
        auto mptr = SPTAG::f_createIO();
        if (mptr == nullptr || !mptr->Initialize(headMapFile.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to open headIDFile file:%s\n", headMapFile.c_str());
            exit(1);
        }

        if (mptr->ReadBinary(sizeof(std::uint64_t) * vec_num, (char*)(m_vectorTranslateMaps[FileNo].get())) != sizeof(std::uint64_t) * vec_num) {
            LOG(Helper::LogLevel::LL_Error, "Cannot Read vector id from Head Vector Map\n");
            exit(0);
        }

        // Loading all metadata

        auto metaData = MemMetadataSet(headMetaFile, headMetaIndexFile, 1024 * 1024, MaxSize, 10);
        if (!(metaData.Available()))
        {
            LOG(Helper::LogLevel::LL_Error, "Error: Failed to load metadata.\n");
            exit(0);
        }

        for (int i = 0; i < vec_num; i++) {

            ByteArray globalVectorID_byteArray = metaData.GetMetadata((m_vectorTranslateMaps[FileNo].get())[i]);

            std::string globalVectorID_string;
            globalVectorID_string.resize(globalVectorID_byteArray.Length());

            memcpy((char*)globalVectorID_string.data(), (char*)globalVectorID_byteArray.Data(), globalVectorID_byteArray.Length());

            std::uint64_t globalVectorID = std::stoull(globalVectorID_string);

            (m_vectorTranslateMaps[FileNo].get())[i] = globalVectorID;

            if (dedupSet.find(globalVectorID) == dedupSet.end()) {
                dedupSet.insert(globalVectorID);
            }
        }
    }

    totalSize = dedupSet.size();

    int eachFileSize = totalSize / options.m_numOutputFiles;
    int writeCount = 0;

    dedupSet.clear();

    int writeFileNo = 0;
    uint64_t begin = 0;
    auto optr_data = SPTAG::f_createIO();
    auto optr_meta = SPTAG::f_createIO();
    auto optr_metaindex = SPTAG::f_createIO();

    int toWriteSize = 0;
    if (writeFileNo == (options.m_numOutputFiles - 1)) {
        toWriteSize = totalSize - totalSize * (options.m_numOutputFiles - 1);
    } else {
        toWriteSize = eachFileSize;
    }

    SetupOutputPtr(writeFileNo, optr_data, optr_meta, optr_metaindex, toWriteSize);
    
    // write new vectors.bin & new metadata file 
    for (int FileNo = 0; FileNo < options.m_numFiles; FileNo++) {

        std::shared_ptr<VectorSet> vectorSet;
        std::string headDataFile = options.inputDirPrefix + std::to_string(FileNo) + FolderSep + "HeadIndex" + FolderSep + "vectors.bin";
        LOG(Helper::LogLevel::LL_Info, "Start loading VectorSet...\n");

        std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(options.m_inputValueType, options.m_dim, SPTAG::VectorFileType::DEFAULT, ""));
        auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
        if (ErrorCode::Success == vectorReader->LoadFile(headDataFile))
        {
            vectorSet = vectorReader->GetVectorSet();
            LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
        }

        int vec_num = vectorSet->Count();
        for (int i = 0; i < vec_num; i++) {
            std::uint64_t globalVectorID = (m_vectorTranslateMaps[FileNo].get())[i];

            if (dedupSet.find(globalVectorID) == dedupSet.end()) {
                dedupSet.insert(globalVectorID);
                //write vector data

                if (optr_data->WriteBinary(sizeof(ValueType) * options.m_dim, (char*)vectorSet->GetVector(i)) != sizeof(ValueType) * options.m_dim) {
                    LOG(Helper::LogLevel::LL_Error, "Write Vector Error!\n");
                    exit(1);
                }
                
                //write meta

                std::string a = std::to_string(globalVectorID);
                if (optr_meta->WriteBinary(a.size(), a.data()) != a.size()) {
                    LOG(Helper::LogLevel::LL_Error, "Write Meta Error!\n");
                    exit(1);
                }

                //write metaindex
                begin += a.size();
                if (optr_metaindex->WriteBinary(sizeof(uint64_t), reinterpret_cast<char*>(&begin)) != sizeof(uint64_t)) {
                    LOG(Helper::LogLevel::LL_Error, "Write Meta Index Error!\n");
                    exit(1);
                }

                writeCount++;
            }

            if (writeCount == toWriteSize && writeFileNo != (options.m_numOutputFiles - 1)) {
                writeCount = 0;
                begin = 0;
                writeFileNo++;
                if (writeFileNo == (options.m_numOutputFiles - 1)) {
                    toWriteSize = totalSize - totalSize * (options.m_numOutputFiles - 1);
                } else {
                    toWriteSize = eachFileSize;
                }
                SetupOutputPtr(writeFileNo, optr_data, optr_meta, optr_metaindex, toWriteSize);
            }
        }
    }
    
}

int main(int argc, char* argv[]) {
    if (!options.Parse(argc - 1, argv + 1))
    {
        exit(1);
    }
    switch (options.m_inputValueType) {
    case SPTAG::VectorValueType::Float:
        MergeHeadSingleNode<float>();
        break;
    case SPTAG::VectorValueType::Int16:
        MergeHeadSingleNode<std::int16_t>();
        break;
    case SPTAG::VectorValueType::Int8:
        MergeHeadSingleNode<std::int8_t>();
        break;
    case SPTAG::VectorValueType::UInt8:
        MergeHeadSingleNode<std::uint8_t>();
        break;
    default:
        LOG(Helper::LogLevel::LL_Error, "Error data type!\n");
    }
    return 0;

}