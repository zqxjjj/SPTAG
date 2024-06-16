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

using namespace SPTAG;

namespace SPTAG {
	namespace MergePosting {
        class MergeOptions : public Helper::ArgumentsParser
        {
        public:
            MergeOptions()
            {
                AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
                AddRequiredOption(m_dim, "-d", "--dimension", "Dimension of vector.");
                AddRequiredOption(m_numNodes, "-n", "--numnodes", "Deploy to n nodes");
                AddRequiredOption(m_numFiles, "-f", "--numfiles", "Merge from f files");
                AddRequiredOption(outputsubFile, "-os", "--outputsubfile", "the prefix of output subfile");
                AddRequiredOption(m_localMergeBufferSize, "-l", "--localmergebuffersize", "Merge l postings in memory for a batch");
                AddRequiredOption(outputNewFile, "-on", "--outputnewfile", "the prefix of output final file");
                AddRequiredOption(m_fileDir, "-fd", "--filedir", "the prefix of to-merge index dir");

                
            }

            ~MergeOptions() {}

            SPTAG::VectorValueType m_inputValueType;
            DimensionType m_dim;
            int m_numNodes;
            int m_numFiles;
            std::string outputsubFile;
            int m_localMergeBufferSize;
            std::string outputNewFile;
            std::string m_fileDir;
            

        } options;

        struct ListInfoSubFile
        {
            std::size_t listTotalBytes = 0;
            
            int listEleCount = 0;

            std::uint16_t listPageCount = 0;

            std::uint64_t listOffset = 0;

            std::uint16_t pageOffset = 0;

            /* it is just for mereging, avoid we scanning the full 46 files in two pass */

            std::uint64_t globalVectorID = 0;
        };

        void SelectPostingOffset(
            const std::vector<size_t>& p_postingListBytes,
            std::unique_ptr<int[]>& p_postPageNum,
            std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
            std::vector<long>& p_postingOrderInIndex)
        {
            p_postPageNum.reset(new int[p_postingListBytes.size()]);
            p_postPageOffset.reset(new std::uint16_t[p_postingListBytes.size()]);

            struct PageModWithID
            {
                long id;

                std::uint16_t rest;
            };

            struct PageModeWithIDCmp
            {
                bool operator()(const PageModWithID& a, const PageModWithID& b) const
                {
                    return a.rest == b.rest ? a.id < b.id : a.rest > b.rest;
                }
            };

            std::set<PageModWithID, PageModeWithIDCmp> listRestSize;

            p_postingOrderInIndex.clear();
            p_postingOrderInIndex.reserve(p_postingListBytes.size());

            PageModWithID listInfo;
            for (size_t i = 0; i < p_postingListBytes.size(); ++i)
            {
                if (p_postingListBytes[i] == 0)
                {
                    continue;
                }

                listInfo.id = static_cast<long>(i);
                listInfo.rest = static_cast<std::uint16_t>(p_postingListBytes[i] % PageSize);

                listRestSize.insert(listInfo);
            }

            listInfo.id = -1;

            int currPageNum = 0;
            std::uint16_t currOffset = 0;

            while (!listRestSize.empty())
            {
                listInfo.rest = PageSize - currOffset;
                auto iter = listRestSize.lower_bound(listInfo); // avoid page-crossing
                if (iter == listRestSize.end() || (listInfo.rest != PageSize && iter->rest == 0))
                {
                    ++currPageNum;
                    currOffset = 0;
                }
                else
                {
                    p_postPageNum[iter->id] = currPageNum;
                    p_postPageOffset[iter->id] = currOffset;

                    p_postingOrderInIndex.push_back(iter->id);

                    currOffset += iter->rest;
                    if (currOffset > PageSize)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Crossing extra pages\n");
                        throw std::runtime_error("Read too many pages");
                    }

                    if (currOffset == PageSize)
                    {
                        ++currPageNum;
                        currOffset = 0;
                    }

                    currPageNum += static_cast<int>(p_postingListBytes[iter->id] / PageSize);

                    listRestSize.erase(iter);
                }
            }

            LOG(Helper::LogLevel::LL_Info, "TotalPageNumbers: %d, IndexSize: %llu\n", currPageNum, static_cast<uint64_t>(currPageNum) * PageSize + currOffset);
        }

        int SelectPostingOffsetLocalBatch(
            const std::vector<size_t>& p_postingListBytes,
            std::unique_ptr<int[]>& p_postPageNum,
            std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
            std::vector<long>& p_postingOrderInIndex,
            int lastPageNum,
            size_t indexBeg,
            int selectNum)
        {

            struct PageModWithID
            {
                long id;

                std::uint16_t rest;
            };

            struct PageModeWithIDCmp
            {
                bool operator()(const PageModWithID& a, const PageModWithID& b) const
                {
                    return a.rest == b.rest ? a.id < b.id : a.rest > b.rest;
                }
            };

            std::set<PageModWithID, PageModeWithIDCmp> listRestSize;

            p_postingOrderInIndex.clear();
            p_postingOrderInIndex.reserve(selectNum);

            PageModWithID listInfo;
            for (size_t i = 0; i < selectNum; ++i)
            {
                if (p_postingListBytes[i + indexBeg] == 0)
                {
                    continue;
                }

                listInfo.id = static_cast<long>(i);
                listInfo.rest = static_cast<std::uint16_t>(p_postingListBytes[i + indexBeg] % PageSize);

                listRestSize.insert(listInfo);
            }

            listInfo.id = -1;

            int currPageNum = lastPageNum + 1;
            std::uint16_t currOffset = 0;

            while (!listRestSize.empty())
            {
                listInfo.rest = PageSize - currOffset;
                auto iter = listRestSize.lower_bound(listInfo); // avoid page-crossing
                if (iter == listRestSize.end() || (listInfo.rest != PageSize && iter->rest == 0))
                {
                    ++currPageNum;
                    currOffset = 0;
                }
                else
                {
                    p_postPageNum[iter->id + indexBeg] = currPageNum;
                    p_postPageOffset[iter->id + indexBeg] = currOffset;

                    p_postingOrderInIndex.push_back(iter->id);

                    currOffset += iter->rest;
                    if (currOffset > PageSize)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Crossing extra pages\n");
                        throw std::runtime_error("Read too many pages");
                    }

                    if (currOffset == PageSize)
                    {
                        ++currPageNum;
                        currOffset = 0;
                    }

                    currPageNum += static_cast<int>(p_postingListBytes[iter->id + indexBeg] / PageSize);

                    listRestSize.erase(iter);
                }
            }

            return currPageNum;
        }

        std::shared_ptr<VectorIndex> LoadingIndex(std::string filename) {
            std::shared_ptr<VectorIndex> m_toLoadIndex;
            if (m_toLoadIndex->LoadIndex(filename, m_toLoadIndex) != ErrorCode::Success) {
                LOG(Helper::LogLevel::LL_Error, "Failed to load index.\n");
                return nullptr;
            }
            return m_toLoadIndex;
        }

        template <typename ValueType>
        void DispatchPostingToNode(SPANN::Index<ValueType>* p_index, std::vector<std::map<size_t, size_t>>& sortHeadByNodeAndGlobalVectorID, MergeOptions& p_opts) {
            // global vectorID module numNodes
            LOG(Helper::LogLevel::LL_Info, "Dispatch Posting to Node\n");
            size_t localTotalHeadNum = p_index->GetMemoryIndex()->GetNumSamples();
            LOG(Helper::LogLevel::LL_Info, "Total Posting Num: %d\n", localTotalHeadNum);
            for (size_t localHeadID = 0; localHeadID < localTotalHeadNum; localHeadID++) {

                ByteArray globalVectorID_byteArray = p_index->GetMetadata(p_index->ReturnTrueId(localHeadID));

                std::string globalVectorID_string;
                globalVectorID_string.resize(globalVectorID_byteArray.Length());

                memcpy((char*)globalVectorID_string.data(), (char*)globalVectorID_byteArray.Data(), globalVectorID_byteArray.Length());

                // LOG(Helper::LogLevel::LL_Info, "Loacal HeadID: %d, Local Vector ID: %d, Global Vector ID: %s\n", localHeadID, p_index->ReturnTrueId(localHeadID), globalVectorID_string.c_str());

                std::uint64_t globalVectorID = std::stoull(globalVectorID_string);

                sortHeadByNodeAndGlobalVectorID[globalVectorID % p_opts.m_numNodes].emplace(globalVectorID, localHeadID);
            }
            
            for (int i = 0; i < p_opts.m_numNodes; i++) {
                LOG(Helper::LogLevel::LL_Info, "Dispatch Posting to Node %d : %d\n", i, sortHeadByNodeAndGlobalVectorID[i].size());
            }

        }

        template <typename ValueType>
        void GenerateSubFileMetaInfo(SPANN::Index<ValueType>* p_index, int NodeNo, size_t& totalSize, 
                std::vector<size_t>& postingTotalBytes_subFile, std::vector<int>& listEleCount_subFile,
                std::vector<size_t>& indexToGlobalvectorIDMap, std::map<size_t, size_t>& sortHeadByNodeAndGlobalVectorID,
                MergeOptions& p_opts) {
            totalSize = 0;
            for (auto iter : sortHeadByNodeAndGlobalVectorID) {
                listEleCount_subFile[totalSize] = p_index->ReturnPostingSize(iter.second);
                /* Vector Size */
                postingTotalBytes_subFile[totalSize] = listEleCount_subFile[totalSize] * (sizeof(size_t) + p_opts.m_dim * sizeof(ValueType));
                /* Record global HeadID in the begining by 8B */
                postingTotalBytes_subFile[totalSize] += sizeof(size_t);
                indexToGlobalvectorIDMap[totalSize] = iter.first;
                totalSize++;
            }
        }

        template <typename ValueType>
        void OutputSubFile(SPANN::Index<ValueType>* p_index, std::string filename, 
                std::unique_ptr<int[]>& postPageNum, std::unique_ptr<std::uint16_t[]>& postPageOffset,
                std::vector<long> postingOrderInIndex, std::vector<size_t>& postingTotalBytes_subFile,
                std::vector<int>& listEleCount_subFile, std::vector<size_t>& indexToGlobalvectorIDMap,
                std::map<size_t, size_t>& sortHeadByNodeAndGlobalVectorID, size_t totalSize,
                MergeOptions& p_opts) {

            auto ptr = SPTAG::f_createIO();
            int retry = 3;

            while (retry > 0 && (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)))
            {
                LOG(Helper::LogLevel::LL_Error, "Failed open file %s, retrying...\n", filename.c_str());
                retry--;
            }

            if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
                LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
                throw std::runtime_error("Failed to open file for SSD index save");
            }

            // meta size of global info, skip dimension and vectors here
            std::uint64_t listOffset = sizeof(size_t) * 2;
            // meta size of the posting lists, add extra 8B for global vector ID
            listOffset += (sizeof(int) + sizeof(std::uint16_t) + sizeof(int) + sizeof(std::uint16_t) + sizeof(size_t)) * totalSize;

            std::unique_ptr<char[]> paddingVals(new char[PageSize]);
            memset(paddingVals.get(), 0, sizeof(char) * PageSize);
            // paddingSize: bytes left in the last page
            std::uint64_t paddingSize = PageSize - (listOffset % PageSize);
            if (paddingSize == PageSize)
            {
                paddingSize = 0;
            }
            else
            {
                listOffset += paddingSize;
            }

            // Number of posting lists
            size_t u64Val = static_cast<size_t>(totalSize);
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Skip Number of vectors

            // Skip Vector dimension

            // Page offset of list content section
            u64Val = static_cast<size_t>(listOffset / PageSize);
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Meta of each posting list
            for (size_t i = 0; i < totalSize; ++i)
            {
                size_t postingListByte = 0;
                int pageNum = 0; // starting page number
                std::uint16_t pageOffset = 0;
                int listEleCount = 0;
                std::uint16_t listPageCount = 0;
                size_t globalVectorID = indexToGlobalvectorIDMap[i];

                if (listEleCount_subFile[i] > 0)
                {
                    pageNum = postPageNum[i];
                    pageOffset = static_cast<std::uint16_t>(postPageOffset[i]);
                    listEleCount = static_cast<int>(listEleCount_subFile[i]);
                    postingListByte = postingTotalBytes_subFile[i];
                    listPageCount = static_cast<std::uint16_t>(postingListByte / PageSize);
                    if (0 != (postingListByte % PageSize))
                    {
                        ++listPageCount;
                    }
                }
                // Page number of the posting list
                if (ptr->WriteBinary(sizeof(pageNum), reinterpret_cast<char*>(&pageNum)) != sizeof(pageNum)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page offset
                if (ptr->WriteBinary(sizeof(pageOffset), reinterpret_cast<char*>(&pageOffset)) != sizeof(pageOffset)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Number of vectors in the posting list
                if (ptr->WriteBinary(sizeof(listEleCount), reinterpret_cast<char*>(&listEleCount)) != sizeof(listEleCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page count of the posting list
                if (ptr->WriteBinary(sizeof(listPageCount), reinterpret_cast<char*>(&listPageCount)) != sizeof(listPageCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Global Vector ID of this posting
                if (ptr->WriteBinary(sizeof(globalVectorID), reinterpret_cast<char*>(&globalVectorID)) != sizeof(globalVectorID)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            // Write padding vals
            if (paddingSize > 0)
            {
                if (ptr->WriteBinary(paddingSize, reinterpret_cast<char*>(paddingVals.get())) != paddingSize) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            if (static_cast<uint64_t>(ptr->TellP()) != listOffset)
            {
                LOG(Helper::LogLevel::LL_Info, "List offset not match!\n");
                throw std::runtime_error("List offset mismatch");
            }

            LOG(Helper::LogLevel::LL_Info, "SubFile: SubIndex Size: %llu bytes, %llu MBytes\n", listOffset, listOffset >> 20);

            /* Begin to write subfiles data */

            listOffset = 0;

            std::uint64_t paddedSize = 0;
            // iterate over all the posting lists
            for (auto id : postingOrderInIndex)
            {
                /* id is the index in sortHeadByNodeAndGlobalVectorID map, we can get it by find the local headID and global vector id in the sortHeadByNodeAndGlobalVectorID map */
                /* To quickly locate in sortHeadByNodeAndGlobalVectorID, we additionally maintain an index-to-globalVectorID map */
                std::uint64_t targetOffset = static_cast<uint64_t>(postPageNum[id]) * PageSize + postPageOffset[id];
                if (targetOffset < listOffset)
                {
                    LOG(Helper::LogLevel::LL_Info, "List offset not match, targetOffset < listOffset!\n");
                    throw std::runtime_error("List offset mismatch");
                }
                // write padding vals before the posting list
                if (targetOffset > listOffset)
                {
                    if (targetOffset - listOffset > PageSize)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Padding size greater than page size!\n");
                        throw std::runtime_error("Padding size mismatch with page size");
                    }

                    if (ptr->WriteBinary(targetOffset - listOffset, reinterpret_cast<char*>(paddingVals.get())) != targetOffset - listOffset) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }

                    paddedSize += targetOffset - listOffset;

                    listOffset = targetOffset;
                }

                if (listEleCount_subFile[id] == 0)
                {
                    continue;
                }
                std::string postingListFullData;
                /*s */
                size_t postingListFullSize = postingTotalBytes_subFile[id];
                postingListFullData.resize(postingListFullSize);
                // get posting list full content and write it at once, the procedure is just similar to the previous section where we using categorization sorting
                /* Get its localHeadId to read posting*/
                size_t postingGlobalVectorID = indexToGlobalvectorIDMap[id];
                int localHeadID = sortHeadByNodeAndGlobalVectorID[postingGlobalVectorID];

                std::string postingListFullData_prev;
                p_index->ReadPosting(localHeadID, postingListFullData_prev);

                /* convert localVectorID to globalVectorID and change to 8B by GetMetadata*/

                char* postingPtr = (char*)postingListFullData.c_str();
                char* postingPtr_prev = (char*)postingListFullData_prev.c_str();

                int postingLength = postingListFullData_prev.size() / (sizeof(int) + sizeof(ValueType) * p_opts.m_dim);

                // if (id == 288) LOG(Helper::LogLevel::LL_Error, "Posting ID: %d, its length: %d\n", id, postingLength);
                for (int i = 0; i < postingLength; i++) {
                    int localVectorID = *(int*)postingPtr_prev;
                    ByteArray globalVectorID_byteArray = p_index->GetMetadata(localVectorID);

                    std::string globalVectorID_string;
                    globalVectorID_string.resize(globalVectorID_byteArray.Length());

                    memcpy((char*)globalVectorID_string.data(), (char*)globalVectorID_byteArray.Data(), globalVectorID_byteArray.Length());

                    std::uint64_t globalVectorID = std::stoull(globalVectorID_string);
                    // if (id == 288) LOG(Helper::LogLevel::LL_Error, "Global ID : %lu\n", globalVectorID);
                    
                    memcpy(postingPtr, (char*)&globalVectorID, sizeof(size_t));
                    postingPtr += sizeof(size_t);

                    memcpy(postingPtr, postingPtr_prev + sizeof(int), sizeof(ValueType) * p_opts.m_dim);

                    postingPtr_prev += sizeof(int);

                    postingPtr_prev += sizeof(ValueType) * p_opts.m_dim;

                    postingPtr += sizeof(ValueType) * p_opts.m_dim;

                }
                
                if (ptr->WriteBinary(postingListFullSize, postingListFullData.data()) != postingListFullSize)
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                listOffset += postingListFullSize;
            }

            paddingSize = PageSize - (listOffset % PageSize);
            if (paddingSize == PageSize)
            {
                paddingSize = 0;
            }
            else
            {
                listOffset += paddingSize;
                paddedSize += paddingSize;
            }

            if (paddingSize > 0)
            {
                if (ptr->WriteBinary(paddingSize, reinterpret_cast<char *>(paddingVals.get())) != paddingSize)
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            LOG(Helper::LogLevel::LL_Info, "Padded Size: %llu, final total size: %llu.\n", paddedSize, listOffset);

            LOG(Helper::LogLevel::LL_Info, "Output done...\n");
        }


        template <typename ValueType>
        void DivideAndWriteSubFiles(SPANN::Index<ValueType>* p_index, std::vector<std::map<size_t, size_t>>& sortHeadByNodeAndGlobalVectorID, MergeOptions& p_opts, int FileNo) {
            for (int NodeNo = 0; NodeNo < p_opts.m_numNodes; NodeNo++) {
                std::vector<size_t> postingTotalBytes_subFile(sortHeadByNodeAndGlobalVectorID[NodeNo].size());
                std::vector<int> listEleCount_subFile(sortHeadByNodeAndGlobalVectorID[NodeNo].size());
                /* local posting id to global vector id map */
                std::vector<size_t> indexToGlobalvectorIDMap(sortHeadByNodeAndGlobalVectorID[NodeNo].size());
                size_t totalSize = 0;
                
                /* Counting postingTotalByte, add extra index info for map */
                GenerateSubFileMetaInfo<ValueType>(p_index, NodeNo, totalSize, postingTotalBytes_subFile, listEleCount_subFile, indexToGlobalvectorIDMap, sortHeadByNodeAndGlobalVectorID[NodeNo], p_opts);
                
                /* generate metadata */
                std::unique_ptr<int[]> postPageNum;
                std::unique_ptr<std::uint16_t[]> postPageOffset;
                std::vector<long> postingOrderInIndex;

                SelectPostingOffset(postingTotalBytes_subFile, postPageNum, postPageOffset, postingOrderInIndex);

                /* Write Metadata */
                LOG(Helper::LogLevel::LL_Info, "Finish metadata generation, start output...\n");

                std::string filename = p_opts.outputsubFile + '_' + std::to_string(FileNo) + '_' + std::to_string(NodeNo);

                OutputSubFile<ValueType>(p_index, filename, postPageNum, postPageOffset, postingOrderInIndex, postingTotalBytes_subFile, listEleCount_subFile, indexToGlobalvectorIDMap,
                sortHeadByNodeAndGlobalVectorID[NodeNo], totalSize, p_opts);
            }
        }

        template <typename ValueType>
        void LoadingAllSubFiles(MergeOptions& p_opts, std::vector<size_t>& m_listCounts,
                std::vector<size_t>& m_listPageOffsets, std::vector<std::shared_ptr<Helper::DiskIO>>& ptrs,
                std::vector<std::vector<ListInfoSubFile>>& m_listInfos, int NodeNo) {

            int m_vectorInfoSize = p_opts.m_dim * sizeof(ValueType) + sizeof(size_t);

            for (int FileNo = 0; FileNo < p_opts.m_numFiles; FileNo++) {
                /* Generate File Name*/
                std::string p_file = p_opts.outputsubFile + '_' + std::to_string(FileNo) + '_' + std::to_string(NodeNo);

                /* Loading Head Info, Loading 8B data requires modification, so I just put loading code here */
                ptrs[FileNo] = SPTAG::f_createIO();
                if (ptrs[FileNo] == nullptr || !ptrs[FileNo]->Initialize(p_file.c_str(), std::ios::binary | std::ios::in)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to open file: %s\n", p_file.c_str());
                    throw std::runtime_error("Failed open file in LoadingHeadInfo when merging (step 4)");
                }
                
                /* Compared to origin head info, here gets no data dimension and total vector information */
                if (ptrs[FileNo]->ReadBinary(sizeof(m_listCounts[FileNo]), reinterpret_cast<char*>(&m_listCounts[FileNo])) != sizeof(m_listCounts[FileNo])) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }

                if (ptrs[FileNo]->ReadBinary(sizeof(m_listPageOffsets[FileNo]), reinterpret_cast<char*>(&m_listPageOffsets[FileNo])) != sizeof(m_listPageOffsets[FileNo])) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }

                m_listInfos[FileNo].resize(m_listCounts[FileNo]);
                int pageNum;
                for (size_t i = 0; i < m_listCounts[FileNo]; ++i) {
                    ListInfoSubFile* listInfo = &(m_listInfos[FileNo][i]);
                    if (ptrs[FileNo]->ReadBinary(sizeof(pageNum), reinterpret_cast<char*>(&(pageNum))) != sizeof(pageNum)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptrs[FileNo]->ReadBinary(sizeof(listInfo->pageOffset), reinterpret_cast<char*>(&(listInfo->pageOffset))) != sizeof(listInfo->pageOffset)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptrs[FileNo]->ReadBinary(sizeof(listInfo->listEleCount), reinterpret_cast<char*>(&(listInfo->listEleCount))) != sizeof(listInfo->listEleCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptrs[FileNo]->ReadBinary(sizeof(listInfo->listPageCount), reinterpret_cast<char*>(&(listInfo->listPageCount))) != sizeof(listInfo->listPageCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }

                    if (ptrs[FileNo]->ReadBinary(sizeof(listInfo->globalVectorID), reinterpret_cast<char*>(&(listInfo->globalVectorID))) != sizeof(listInfo->globalVectorID)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    listInfo->listOffset = (static_cast<uint64_t>(m_listPageOffsets[FileNo] + pageNum) << PageSizeEx);
                    listInfo->listTotalBytes = listInfo->listEleCount * m_vectorInfoSize;
                    // listInfo->listEleCount = min(listInfo->listEleCount, (min(static_cast<int>(listInfo->listPageCount), p_postingPageLimit) << PageSizeEx) / m_vectorInfoSize);
                    listInfo->listPageCount = static_cast<std::uint16_t>(ceil((m_vectorInfoSize * listInfo->listEleCount + listInfo->pageOffset) * 1.0 / (1 << PageSizeEx)));
                }
            }
        }

        void FetchPosting(std::shared_ptr<Helper::DiskIO>& ptr, ListInfoSubFile& listInfo, std::string& posting, size_t m_vectorInfoSize) {
            size_t totalBytes = (static_cast<size_t>(listInfo.listPageCount) << PageSizeEx);
            size_t realBytes = listInfo.listEleCount * m_vectorInfoSize;
            posting.resize(totalBytes);
            auto numRead = ptr->ReadBinary(totalBytes, posting.data(), listInfo.listOffset);
            if (numRead != totalBytes) {
                LOG(Helper::LogLevel::LL_Error, "File read bytes, expected: %zu, acutal: %llu.\n", totalBytes, numRead);
                throw std::runtime_error("File read mismatch");
            }
            char* pptr = (char*)(posting.c_str());
            memcpy(pptr, posting.c_str() + listInfo.pageOffset, realBytes);
            posting.resize(realBytes);
        }

        template <typename ValueType>   
        void MergeAndWriteFiles(MergeOptions& p_opts, std::vector<size_t>& m_listCounts,
                std::vector<size_t>& m_listPageOffsets, std::vector<std::shared_ptr<Helper::DiskIO>>& ptrs,
                std::vector<std::vector<ListInfoSubFile>>& m_listInfos, int NodeNo) {

            /* 5. Merge those files into one SSDIndexFilesNew_{NodeID} */
            /* Using Merge Sorting */
            /* To avoid limited by memory, the merging progress involves first pre-process the meta information and then really merge the data into the new files */
            
            /* 5.1 Pre-process the meta information */

            int preTotalSize = 0;
            for (int FileNo = 0; FileNo < p_opts.m_numFiles; FileNo++) {
                preTotalSize += m_listCounts[FileNo];
            }

            struct Node {
                size_t globalVectorID;
                int fileNo;
                size_t listIndex;

                Node(size_t id, int fn, size_t li) : globalVectorID(id), fileNo(fn), listIndex(li) {}
            };

            struct CompareNode {
                bool operator()(Node const& n1, Node const& n2) {
                    return n1.globalVectorID == n2.globalVectorID ? n1.fileNo > n2.fileNo : n1.globalVectorID > n2.globalVectorID;
                }
            };

            LOG(Helper::LogLevel::LL_Info, "Start sorting\n");
            /* Here we just want to know the real posting num after merge*/
            std::priority_queue<Node, std::vector<Node>, CompareNode> merge_queue;
            for (int FileNo = 0; FileNo < p_opts.m_numFiles; FileNo++) {
                merge_queue.push(Node(m_listInfos[FileNo][0].globalVectorID, FileNo, 0));
            }

            std::vector<Node> merge_result;
            while (!merge_queue.empty()) {
                /* Select Min Global Vector ID */
                Node node = merge_queue.top();
                merge_queue.pop();
                merge_result.push_back(node);
                if (node.listIndex +1 < m_listCounts[node.fileNo]) {
                    merge_queue.push(Node(m_listInfos[node.fileNo][node.listIndex + 1].globalVectorID, node.fileNo, node.listIndex +1));
                }
            }

            size_t totalSize = 1;
            size_t prevGlobalID = merge_result[0].globalVectorID;

            for (size_t i = 1; i < merge_result.size(); i++) {
                if (prevGlobalID != merge_result[i].globalVectorID) {
                    totalSize++;
                }
                prevGlobalID = merge_result[i].globalVectorID;
            }

            LOG(Helper::LogLevel::LL_Info, "Sorting Completed\n");

            /* now we get total Size, begin online merge */

            std::string filename = p_opts.outputNewFile + '_' + std::to_string(NodeNo);

            auto ptr = SPTAG::f_createIO();
            int retry = 3;
            // open file 
            while (retry > 0 && (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)))
            {
                LOG(Helper::LogLevel::LL_Error, "Failed open file %s, retrying...\n", filename.c_str());
                retry--;
            }

            if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
                LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
                throw std::runtime_error("Failed to open file for SSD index save");
            }

            /* Metadata size */
            // meta size of global info: 3*8B for Number of posting lists, Page offset of list content section and vector count(but currently set to 0), 4B for vector dimension 
            std::uint64_t listOffset = sizeof(size_t) * 3 + sizeof(int);
            // meta size of the posting lists
            listOffset += (sizeof(int) + sizeof(std::uint16_t) + sizeof(int) + sizeof(std::uint16_t) + sizeof(size_t)) * totalSize;

            // padding for metadata
            std::unique_ptr<char[]> paddingVals(new char[PageSize]);
            memset(paddingVals.get(), 0, sizeof(char) * PageSize);
            // paddingSize: bytes left in the last page
            std::uint64_t paddingSize = PageSize - (listOffset % PageSize);
            if (paddingSize == PageSize)
            {
                paddingSize = 0;
            }
            else
            {
                listOffset += paddingSize;
            }

            // Begin with writing useless data for metadata space (it is ugly, maybe we can polish it later)

            // Number of posting lists
            uint64_t u64Val = totalSize;
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Number of vectors, current we set it as 0 since we don't know how many vectors are on this node
            u64Val = 0;
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Vector dimension
            int i32Val = static_cast<int>(p_opts.m_dim);
            if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Page offset of list content section
            u64Val = static_cast<uint64_t>(listOffset / PageSize);
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Meta of each posting list
            for (int i = 0; i < totalSize; ++i)
            {
                size_t postingListByte = 0;
                int pageNum = 0; // starting page number
                std::uint16_t pageOffset = 0;
                int listEleCount = 0;
                std::uint16_t listPageCount = 0;
                size_t globalVectorID = 0;

                // Page number of the posting list
                if (ptr->WriteBinary(sizeof(pageNum), reinterpret_cast<char*>(&pageNum)) != sizeof(pageNum)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page offset
                if (ptr->WriteBinary(sizeof(pageOffset), reinterpret_cast<char*>(&pageOffset)) != sizeof(pageOffset)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Number of vectors in the posting list
                if (ptr->WriteBinary(sizeof(listEleCount), reinterpret_cast<char*>(&listEleCount)) != sizeof(listEleCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page count of the posting list
                if (ptr->WriteBinary(sizeof(listPageCount), reinterpret_cast<char*>(&listPageCount)) != sizeof(listPageCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Global vector ID
                if (ptr->WriteBinary(sizeof(globalVectorID), reinterpret_cast<char*>(&globalVectorID)) != sizeof(globalVectorID)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            // Write padding vals
            if (paddingSize > 0)
            {
                if (ptr->WriteBinary(paddingSize, reinterpret_cast<char*>(paddingVals.get())) != paddingSize) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            if (static_cast<uint64_t>(ptr->TellP()) != listOffset)
            {
                LOG(Helper::LogLevel::LL_Info, "List offset not match! (%lu/%lu)\n", listOffset, ptr->TellP());
                throw std::runtime_error("List offset mismatch");
            }

            int m_vectorInfoSize = p_opts.m_dim * sizeof(ValueType) + sizeof(size_t);

            // now we begin on online merging
            LOG(Helper::LogLevel::LL_Info, "Begin merge online\n");

            std::vector<size_t> postingListBytes(totalSize);
            std::unique_ptr<int[]> postPageNum(new int[totalSize]);
            std::unique_ptr<std::uint16_t[]> postPageOffset(new std::uint16_t[totalSize]);
            std::vector<size_t> localHeadIDToGlobalVectorIDMap(totalSize);

            std::vector<long> postingOrderInIndex(p_opts.m_localMergeBufferSize);
            std::vector<std::string> localMergeBuffer(p_opts.m_localMergeBufferSize);
            int lastPageNum = -1;

            size_t index = 0;
            int toWritePostingNum = 0;
            size_t postingIndex = 0;
            size_t batchBeg = 0;
            std::uint64_t metalistOffset = listOffset;
            listOffset = 0;

            size_t tempFlag = -1;

            while (index < merge_result.size()) {
                /* First considering merging*/
                size_t indexBeg = index;
                Node toMergePosting = merge_result[index];
                index++;
                while (index < merge_result.size() && merge_result[index].globalVectorID == toMergePosting.globalVectorID) {
                    index++;
                }
                size_t indexEnd = index;
                std::string toWritePosting;
                if (indexEnd != (indexBeg + 1)) {
                    // LOG(Helper::LogLevel::LL_Info, "merging\n");
                    // require merging, need to copy into a new string
                    size_t postingPreSize = 0;
                    for (size_t i = indexBeg; i < indexEnd; i++) {
                        Node tempNode = merge_result[i];
                        postingPreSize += m_listInfos[tempNode.fileNo][tempNode.listIndex].listEleCount;
                    }
                    toWritePosting.resize(postingPreSize * m_vectorInfoSize);
                    char* mptr = (char*)(toWritePosting.c_str());
                    std::set<size_t> VIDset;
                    for (size_t i = indexBeg; i < indexEnd; i++) {
                        Node tempNode = merge_result[i];
                        std::string currentPosting;
                        // LOG(Helper::LogLevel::LL_Info, "fetching\n");
                        FetchPosting(ptrs[tempNode.fileNo], m_listInfos[tempNode.fileNo][tempNode.listIndex], currentPosting, m_vectorInfoSize);
                        char* pptr = (char*)(currentPosting.c_str());
                        // Scanning and Copying
                        // if (tempNode.listIndex == 288) LOG(Helper::LogLevel::LL_Info, "scanning, vectorNum : %d, actual vectorNum : %d, it is file : %d, ith : posting : %d\n", (currentPosting.size()/m_vectorInfoSize), m_listInfos[tempNode.fileNo][tempNode.listIndex].listEleCount, tempNode.fileNo, tempNode.listIndex);
                        for (int vectorNum = (currentPosting.size()/m_vectorInfoSize), i = 0; i < vectorNum; i++) {
                            char* vectorInfo = pptr + i * (m_vectorInfoSize);
                            size_t VID = *(reinterpret_cast<size_t*>(vectorInfo));
                            // if (tempNode.listIndex == 288) LOG(Helper::LogLevel::LL_Info, "VID: %lu\n", VID);
                            if (VIDset.find(VID) == VIDset.end()) {
                                VIDset.insert(VID);
                                memcpy(mptr, pptr, m_vectorInfoSize);
                                mptr += m_vectorInfoSize;
                            } else {
                                postingPreSize--;
                            }
                        }
                    }
                    toWritePosting.resize(postingPreSize * m_vectorInfoSize);
                    // LOG(Helper::LogLevel::LL_Info, "merging finished\n");
                } else {
                    // LOG(Helper::LogLevel::LL_Info, "No need to merge, directly fetch\n");
                    // otherwise we just read it, then considering how to place it 
                    Node tempNode = merge_result[indexBeg];
                    FetchPosting(ptrs[tempNode.fileNo], m_listInfos[tempNode.fileNo][tempNode.listIndex], toWritePosting, m_vectorInfoSize);
                }
                
                /* now we get the merged posting, next we need to consider how to place it */
                /* since every posting is about 4KB ~ 100KB(? maybe more), we can consider buffering some postings and considering writing them together */

                localMergeBuffer[toWritePostingNum] = toWritePosting;
                postingListBytes[postingIndex] = toWritePosting.size();
                localHeadIDToGlobalVectorIDMap[postingIndex] = toMergePosting.globalVectorID;
                // if (postingIndex == 22254 && NodeNo == 0) {
                //     tempFlag = toWritePostingNum;
                //     char* pptr = (char*)(toWritePosting.c_str()); 
                //     LOG(Helper::LogLevel::LL_Info, "Global VID: %lu, size: %d\n", toMergePosting.globalVectorID, (toWritePosting.size()/m_vectorInfoSize));
                //     for (int vectorNum = (toWritePosting.size()/m_vectorInfoSize), i = 0; i < vectorNum; i++) {
                //         char* vectorInfo = pptr + i * (m_vectorInfoSize);
                //         size_t VID = *(reinterpret_cast<size_t*>(vectorInfo));
                //         LOG(Helper::LogLevel::LL_Info, "VID: %lu\n", VID);
                        
                //     }
                // }

                toWritePostingNum++;
                postingIndex++;

                if (toWritePostingNum == p_opts.m_localMergeBufferSize) {
                    // LOG(Helper::LogLevel::LL_Info, "Batch full, begin to arrange writting\n");
                    /* Now the buffer is full, let's considering merge order */
                    lastPageNum = SelectPostingOffsetLocalBatch(postingListBytes, postPageNum, postPageOffset, postingOrderInIndex, lastPageNum, batchBeg, p_opts.m_localMergeBufferSize);

                    /* Now we get write order, then write into files, padding the batch end */

                    std::uint64_t paddedSize = 0;
                    // iterate over all the batched posting lists
                    for (auto id : postingOrderInIndex)
                    {
                        // LOG(Helper::LogLevel::LL_Info, "id: %d, its pageNum is: %d, postPageOffset:%d, size: %d\n", id, postPageNum[id + batchBeg], postPageOffset[id + batchBeg], postingListBytes[id + batchBeg]);
                        std::uint64_t targetOffset = static_cast<uint64_t>(postPageNum[id + batchBeg]) * PageSize + postPageOffset[id + batchBeg];
                        if (targetOffset < listOffset)
                        {
                            LOG(Helper::LogLevel::LL_Info, "List offset not match, targetOffset(%lu) < listOffset(%lu)!\n", targetOffset, listOffset);
                            throw std::runtime_error("List offset mismatch");
                        }
                        // write padding vals before the posting list
                        if (targetOffset > listOffset)
                        {
                            if (targetOffset - listOffset > PageSize)
                            {
                                LOG(Helper::LogLevel::LL_Error, "Padding size greater than page size!\n");
                                throw std::runtime_error("Padding size mismatch with page size");
                            }

                            if (ptr->WriteBinary(targetOffset - listOffset, reinterpret_cast<char*>(paddingVals.get())) != targetOffset - listOffset) {
                                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                                throw std::runtime_error("Failed to write SSDIndex File");
                            }

                            paddedSize += targetOffset - listOffset;

                            listOffset = targetOffset;
                        }

                        if (postingListBytes[id + batchBeg] == 0)
                        {
                            continue;
                        }
                        // if (id == tempFlag) {
                        //     LOG(Helper::LogLevel::LL_Info, "22254 posting's pageNum is %d, target offset: %lu\n", postPageNum[id + batchBeg], targetOffset);
                        //     LOG(Helper::LogLevel::LL_Info, "MetaOffset: %lu, Current offset: %lu", metalistOffset, ptr->TellP());
                        //     char* pptr = (char*)(localMergeBuffer[id].c_str()); 
                        //     for (int vectorNum = (localMergeBuffer[id].size()/m_vectorInfoSize), i = 0; i < vectorNum; i++) {
                        //         char* vectorInfo = pptr + i * (m_vectorInfoSize);
                        //         size_t VID = *(reinterpret_cast<size_t*>(vectorInfo));
                        //         LOG(Helper::LogLevel::LL_Info, "Temp flag VID: %lu\n", VID);
                                
                        //     }
                        //     tempFlag = -1;
                        // }
                        
                        if (ptr->WriteBinary(postingListBytes[id + batchBeg], localMergeBuffer[id].data()) != postingListBytes[id + batchBeg])
                        {
                            LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                        listOffset += postingListBytes[id + batchBeg];
                    }

                    // padding in the batch end
                    paddingSize = PageSize - (listOffset % PageSize);
                    if (paddingSize == PageSize)
                    {
                        paddingSize = 0;
                    }
                    else
                    {
                        listOffset += paddingSize;
                        paddedSize += paddingSize;
                    }
                    // LOG(Helper::LogLevel::LL_Info, "Padding in the batchend: %d, current offset: %d\n", paddingSize, listOffset);

                    if (paddingSize > 0)
                    {
                        if (ptr->WriteBinary(paddingSize, reinterpret_cast<char *>(paddingVals.get())) != paddingSize)
                        {
                            LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                    }

                    batchBeg += p_opts.m_localMergeBufferSize;
                    toWritePostingNum = 0;
                    // LOG(Helper::LogLevel::LL_Info, "Remaining %d\n", totalSize - batchBeg);
                }
            }
            // LOG(Helper::LogLevel::LL_Info, "mergeing the last batch\n");
            /* The final merge buffer need to clear */
            postingOrderInIndex.resize(postingIndex - batchBeg);

            lastPageNum = SelectPostingOffsetLocalBatch(postingListBytes, postPageNum, postPageOffset, postingOrderInIndex, lastPageNum, batchBeg, postingIndex - batchBeg);

            /* Now we get write order, then write into files, padding the batch end */

            std::uint64_t paddedSize = 0;
            // iterate over all the batched posting lists
            for (auto id : postingOrderInIndex)
            {
                std::uint64_t targetOffset = static_cast<uint64_t>(postPageNum[id + batchBeg]) * PageSize + postPageOffset[id + batchBeg];
                if (targetOffset < listOffset)
                {
                    LOG(Helper::LogLevel::LL_Info, "List offset not match, targetOffset < listOffset!\n");
                    throw std::runtime_error("List offset mismatch");
                }
                // write padding vals before the posting list
                if (targetOffset > listOffset)
                {
                    if (targetOffset - listOffset > PageSize)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Padding size greater than page size!\n");
                        throw std::runtime_error("Padding size mismatch with page size");
                    }

                    if (ptr->WriteBinary(targetOffset - listOffset, reinterpret_cast<char*>(paddingVals.get())) != targetOffset - listOffset) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }

                    paddedSize += targetOffset - listOffset;

                    listOffset = targetOffset;
                }

                if (postingListBytes[id + batchBeg] == 0)
                {
                    continue;
                }
                
                if (ptr->WriteBinary(postingListBytes[id + batchBeg], localMergeBuffer[id].data()) != postingListBytes[id + batchBeg])
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                listOffset += postingListBytes[id + batchBeg];
            }

            // padding in the batch end
            paddingSize = PageSize - (listOffset % PageSize);
            if (paddingSize == PageSize)
            {
                paddingSize = 0;
            }
            else
            {
                listOffset += paddingSize;
                paddedSize += paddingSize;
            }

            if (paddingSize > 0)
            {
                if (ptr->WriteBinary(paddingSize, reinterpret_cast<char *>(paddingVals.get())) != paddingSize)
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

            LOG(Helper::LogLevel::LL_Info, "Data writing finish, come back to the begining\n");
            /* now back to the beginning to write metadata */

            // Here I do not write a wrap function for seekg, so I begin with write metalistOffset again
            u64Val = static_cast<uint64_t>(metalistOffset / PageSize);
            if (ptr->WriteBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val), sizeof(size_t) * 2 + sizeof(int)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }

            // Meta of each posting list
            for (int i = 0; i < totalSize; ++i)
            {
                size_t postingListByte = postingListBytes[i];
                int pageNum = postPageNum[i]; // starting page number
                std::uint16_t pageOffset = postPageOffset[i];
                int listEleCount = postingListBytes[i] / m_vectorInfoSize;
                std::uint16_t listPageCount = static_cast<std::uint16_t>(postingListByte / PageSize);
                if (0 != (postingListByte % PageSize))
                {
                    ++listPageCount;
                }
                size_t globalVectorID = localHeadIDToGlobalVectorIDMap[i];

                // Page number of the posting list
                // if (i == 22254 && NodeNo == 0) LOG(Helper::LogLevel::LL_Info, "22254 posting's pageNum is %d\n", pageNum);
                if (ptr->WriteBinary(sizeof(pageNum), reinterpret_cast<char*>(&pageNum)) != sizeof(pageNum)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page offset
                if (ptr->WriteBinary(sizeof(pageOffset), reinterpret_cast<char*>(&pageOffset)) != sizeof(pageOffset)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Number of vectors in the posting list
                if (ptr->WriteBinary(sizeof(listEleCount), reinterpret_cast<char*>(&listEleCount)) != sizeof(listEleCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Page count of the posting list
                if (ptr->WriteBinary(sizeof(listPageCount), reinterpret_cast<char*>(&listPageCount)) != sizeof(listPageCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                // Global vector ID
                // if (i == 22254 && NodeNo == 0) LOG(Helper::LogLevel::LL_Info, "22254 posting's global vector ID is %d\n", globalVectorID);
                if (ptr->WriteBinary(sizeof(globalVectorID), reinterpret_cast<char*>(&globalVectorID)) != sizeof(globalVectorID)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
            }

        }

        template <typename ValueType>
        void LoadingNewFiles(MergeOptions& p_opts, int NodeNo) {
            std::string filename = p_opts.outputNewFile + '_' + std::to_string(NodeNo);
            int m_vectorInfoSize = p_opts.m_dim * sizeof(ValueType) + sizeof(size_t);
            auto ptr = SPTAG::f_createIO();

            if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
                LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
                throw std::runtime_error("Failed to open file for SSD index save");
            }

            size_t m_listCount, m_listPageOffset;
            std::vector<ListInfoSubFile> m_listInfo;

            /* Compared to origin head info, here gets no data dimension and total vector information */
            if (ptr->ReadBinary(sizeof(m_listCount), reinterpret_cast<char*>(&m_listCount)) != sizeof(m_listCount)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }

            // Number of vectors, current we set it as 0 since we don't know how many vectors are on this node
            size_t u64Val;
            if (ptr->ReadBinary(sizeof(u64Val), reinterpret_cast<char*>(&u64Val)) != sizeof(u64Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }

            // Vector dimension
            int i32Val;
            if (ptr->ReadBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }

            if (ptr->ReadBinary(sizeof(m_listPageOffset), reinterpret_cast<char*>(&m_listPageOffset)) != sizeof(m_listPageOffset)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }

            m_listInfo.resize(m_listCount);
            int pageNum;
            for (size_t i = 0; i < m_listCount; ++i) {
                ListInfoSubFile* listInfo = &(m_listInfo[i]);
                if (ptr->ReadBinary(sizeof(pageNum), reinterpret_cast<char*>(&(pageNum))) != sizeof(pageNum)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(listInfo->pageOffset), reinterpret_cast<char*>(&(listInfo->pageOffset))) != sizeof(listInfo->pageOffset)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(listInfo->listEleCount), reinterpret_cast<char*>(&(listInfo->listEleCount))) != sizeof(listInfo->listEleCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(listInfo->listPageCount), reinterpret_cast<char*>(&(listInfo->listPageCount))) != sizeof(listInfo->listPageCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }

                if (ptr->ReadBinary(sizeof(listInfo->globalVectorID), reinterpret_cast<char*>(&(listInfo->globalVectorID))) != sizeof(listInfo->globalVectorID)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                listInfo->listOffset = (static_cast<uint64_t>(m_listPageOffset + pageNum) << PageSizeEx);
                listInfo->listTotalBytes = listInfo->listEleCount * m_vectorInfoSize;
                // listInfo->listEleCount = min(listInfo->listEleCount, (min(static_cast<int>(listInfo->listPageCount), p_postingPageLimit) << PageSizeEx) / m_vectorInfoSize);
                listInfo->listPageCount = static_cast<std::uint16_t>(ceil((m_vectorInfoSize * listInfo->listEleCount + listInfo->pageOffset) * 1.0 / (1 << PageSizeEx)));
            }

            LOG(Helper::LogLevel::LL_Info, "Verifying postings randomly for ten times\n");
            for (int i = 0; i < 10; i++) {
                size_t randID = COMMON::Utils::rand(m_listCount, 0);
                ListInfoSubFile listInfo = m_listInfo[randID];
                LOG(Helper::LogLevel::LL_Info, "%lu posting, its global vector id is %lu\n", randID, listInfo.globalVectorID);
                LOG(Helper::LogLevel::LL_Info, "Its size: %d\n", listInfo.listEleCount);
                LOG(Helper::LogLevel::LL_Info, "Its target offset: %lu\n", listInfo.listOffset);

                std::string verifyPosting;

                FetchPosting(ptr, listInfo, verifyPosting, m_vectorInfoSize);

                char* pptr = (char*)(verifyPosting.c_str());
                for (int vectorNum = (verifyPosting.size()/m_vectorInfoSize), i = 0; i < vectorNum; i++) {
                    char* vectorInfo = pptr + i * (m_vectorInfoSize);
                    size_t VID = *(reinterpret_cast<size_t*>(vectorInfo));
                    LOG(Helper::LogLevel::LL_Info,"VectorID : %lu\n", VID);
                }
            }

        }

        template <typename ValueType>
        void MergeSPectrumSingleNode() {
            for (int FileNo = 0; FileNo < options.m_numFiles; FileNo++) {

                LOG(Helper::LogLevel::LL_Info, "Loading File No: %d\n", FileNo);
                /* Loading Index */

                std::shared_ptr<VectorIndex> m_toLoadIndex;

                std::string filename = options.m_fileDir + std::to_string(FileNo);

                m_toLoadIndex = LoadingIndex(filename);

                auto m_toMergeIndex = (SPANN::Index<ValueType>*)m_toLoadIndex.get();

                /* Decide which node that each postin should be dispatched */

                std::vector<std::map<size_t, size_t>> sortHeadByNodeAndGlobalVectorID(options.m_numNodes);

                DispatchPostingToNode<ValueType>(m_toMergeIndex, sortHeadByNodeAndGlobalVectorID, options);

                /* Divide Files into options.m_numNodes files*/

                DivideAndWriteSubFiles<ValueType>(m_toMergeIndex, sortHeadByNodeAndGlobalVectorID, options, FileNo);

                LOG(Helper::LogLevel::LL_Info, "Processing File No: %d Completed\n", FileNo);
            }

            for (int NodeNo = 0; NodeNo < options.m_numNodes; NodeNo++) {
                LOG(Helper::LogLevel::LL_Info, "Processing Node No: %d\n", NodeNo);
                
                /* Loading all subFiles of there metadata & global vector id*/
                std::vector<size_t> m_listCounts(options.m_numFiles);
                std::vector<size_t> m_listPageOffsets(options.m_numFiles);
                std::vector<std::shared_ptr<Helper::DiskIO>> ptrs(options.m_numFiles);
                std::vector<std::vector<ListInfoSubFile>> m_listInfos(options.m_numFiles);
                
                LoadingAllSubFiles<ValueType>(options, m_listCounts, m_listPageOffsets, ptrs, m_listInfos, NodeNo);

                /* Merge and write the merged postings into files */

                MergeAndWriteFiles<ValueType>(options, m_listCounts, m_listPageOffsets, ptrs, m_listInfos, NodeNo);

                LOG(Helper::LogLevel::LL_Info, "Processing Node No: %d Completed\n", NodeNo);
            }

            // Here is some commented code for verify whether the written file is in right form
            // LoadingNewFiles<ValueType>(options, 0);

        }

        int BootProgram(int p_restArgc, char** (p_args)) {
            /* Initialize configuration */
            if (!options.Parse(p_restArgc, p_args))
            {
                exit(1);
            }

            switch (options.m_inputValueType) {
            case SPTAG::VectorValueType::Float:
                MergeSPectrumSingleNode<float>();
                break;
            case SPTAG::VectorValueType::Int16:
                MergeSPectrumSingleNode<std::int16_t>();
                break;
            case SPTAG::VectorValueType::Int8:
                MergeSPectrumSingleNode<std::int8_t>();
                break;
            case SPTAG::VectorValueType::UInt8:
                MergeSPectrumSingleNode<std::uint8_t>();
                break;
            default:
                LOG(Helper::LogLevel::LL_Error, "Error data type!\n");
            }
            return 0;

        }
    }
}