// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_INDEX_H_
#define _SPTAG_SPANN_INDEX_H_

#include "inc/Core/Common.h"
#include "inc/Core/VectorIndex.h"

#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/SIMDUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/Common/BKTree.h"
#include "inc/Core/Common/WorkSpacePool.h"

#include "inc/Core/Common/Labelset.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/ThreadPool.h"
#include "inc/Helper/ConcurrentSet.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Core/Common/IQuantizer.h"

#include "IExtraSearcher.h"
#include "Options.h"

#include <functional>
#include <ratio>
#include <shared_mutex>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>


namespace SPTAG
{

    namespace Helper
    {
        class IniReader;
    }

    namespace SPANN
    {
        template<typename T>
        class Index : public VectorIndex
        {
            class ClientJob : public Helper::ThreadPool::Job
            {
            private:
                int client_socket;
                SPANN::Index<T>* m_index;
                std::function<void()> m_callback;
            public:
                ClientJob(int client_socket, SPANN::Index<T>* m_index, std::function<void()> p_callback)
                    : client_socket(client_socket), m_index(m_index), m_callback(std::move(p_callback)) {}

                ~ClientJob() {}

                inline void exec(IAbortOperation* p_abort) override {
                    m_index->HandleClient(client_socket);
                    if (m_callback != nullptr) {
                        m_callback();
                    }
                }
            };

        private:
            std::shared_ptr<VectorIndex> m_index;
            std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
            std::unordered_map<std::string, std::string> m_headParameters;

            std::shared_ptr<IExtraSearcher> m_extraSearcher;

            Options m_options;

            std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
            int m_iBaseSquare;

            std::mutex m_dataAddLock;
            COMMON::VersionLabel m_versionMap;

            // If not Coord, than bind some port
            bool m_isCoordinator;

            std::shared_ptr<Helper::ThreadPool> m_clientThreadPool;

        public:
            static thread_local std::shared_ptr<ExtraWorkSpace> m_workspace;

        public:
            Index()
            {
                m_fComputeDistance = std::function<float(const T*, const T*, DimensionType)>(COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod));
                m_iBaseSquare = (m_options.m_distCalcMethod == DistCalcMethod::Cosine) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() : 1;
            }

            ~Index() {}

            inline std::shared_ptr<VectorIndex> GetMemoryIndex() { return m_index; }
            inline std::shared_ptr<IExtraSearcher> GetDiskIndex() { return m_extraSearcher; }
            inline Options* GetOptions() { return &m_options; }

            inline SizeType GetNumSamples() const { return m_versionMap.Count(); }
            inline DimensionType GetFeatureDim() const { return m_pQuantizer ? m_pQuantizer->ReconstructDim() : m_index->GetFeatureDim(); }
            inline SizeType GetValueSize() const { return m_options.m_dim * sizeof(T); }

            inline int GetCurrMaxCheck() const { return m_options.m_maxCheck; }
            inline int GetNumThreads() const { return m_options.m_iSSDNumberOfThreads; }
            inline DistCalcMethod GetDistCalcMethod() const { return m_options.m_distCalcMethod; }
            inline IndexAlgoType GetIndexAlgoType() const { return IndexAlgoType::SPANN; }
            inline VectorValueType GetVectorValueType() const { return GetEnumValueType<T>(); }
            
            void SetQuantizer(std::shared_ptr<SPTAG::COMMON::IQuantizer> quantizer);

            inline float AccurateDistance(const void* pX, const void* pY) const { 
                if (m_options.m_distCalcMethod == DistCalcMethod::L2) return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);

                float xy = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
                float xx = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pX, m_options.m_dim);
                float yy = m_iBaseSquare - m_fComputeDistance((const T*)pY, (const T*)pY, m_options.m_dim);
                return 1.0f - xy / (sqrt(xx) * sqrt(yy));
            }
            inline float ComputeDistance(const void* pX, const void* pY) const { return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim); }
            inline bool ContainSample(const SizeType idx) const { return idx < m_options.m_vectorSize; }

            std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const
            {
                std::shared_ptr<std::vector<std::uint64_t>> buffersize(new std::vector<std::uint64_t>);
                auto headIndexBufferSize = m_index->BufferSize();
                buffersize->insert(buffersize->end(), headIndexBufferSize->begin(), headIndexBufferSize->end());
                buffersize->push_back(sizeof(long long) * m_index->GetNumSamples());
                return std::move(buffersize);
            }

            std::shared_ptr<std::vector<std::string>> GetIndexFiles() const
            {
                std::shared_ptr<std::vector<std::string>> files(new std::vector<std::string>);
                auto headfiles = m_index->GetIndexFiles();
                for (auto file : *headfiles) {
                    files->push_back(m_options.m_headIndexFolder + FolderSep + file);
                }
                if (m_options.m_excludehead) files->push_back(m_options.m_headIDFile);
                return std::move(files);
            }

            ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout);
            ErrorCode SaveIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);

            ErrorCode LoadConfig(Helper::IniReader& p_reader);
            ErrorCode LoadIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
            ErrorCode LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs);

            ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, bool p_normalized = false, bool p_shareOwnership = false);
            ErrorCode BuildIndex(bool p_normalized = false);
            ErrorCode SearchIndex(QueryResult &p_query, bool p_searchDeleted = false) const;
            ErrorCode SearchDiskIndex(QueryResult& p_query, SearchStats* p_stats = nullptr) const;
            ErrorCode DebugSearchDiskIndex(QueryResult& p_query, int p_subInternalResultNum, int p_internalResultNum,
                SearchStats* p_stats = nullptr, std::set<int>* truth = nullptr, std::map<int, std::set<int>>* found = nullptr) const;
            ErrorCode UpdateIndex();

            ErrorCode SetParameter(const char* p_param, const char* p_value, const char* p_section = nullptr);
            std::string GetParameter(const char* p_param, const char* p_section = nullptr) const;

            inline const void* GetSample(const SizeType idx) const { return nullptr; }
            inline SizeType GetNumDeleted() const { return m_versionMap.GetDeleteCount(); }
            inline bool NeedRefine() const { return false; }
            ErrorCode RefineSearchIndex(QueryResult &p_query, bool p_searchDeleted = false) const { return ErrorCode::Undefined; }
            ErrorCode SearchTree(QueryResult& p_query) const { return ErrorCode::Undefined; }
            ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex = false, bool p_normalized = false);
            ErrorCode DeleteIndex(const SizeType& p_id);

            ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum);
            ErrorCode RefineIndex(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams, IAbortOperation* p_abort) { return ErrorCode::Undefined; }
            ErrorCode RefineIndex(std::shared_ptr<VectorIndex>& p_newIndex) { return ErrorCode::Undefined; }
            
        private:
            bool CheckHeadIndexType();
            void SelectHeadAdjustOptions(int p_vectorCount);
            int SelectHeadDynamicallyInternal(const std::shared_ptr<COMMON::BKTree> p_tree, int p_nodeID, const Options& p_opts, std::vector<int>& p_selected);
            void SelectHeadDynamically(const std::shared_ptr<COMMON::BKTree> p_tree, int p_vectorCount, std::vector<int>& p_selected);

            template <typename InternalDataType>
            bool SelectHeadInternal(std::shared_ptr<Helper::VectorSetReader>& p_reader);

            ErrorCode BuildIndexInternal(std::shared_ptr<Helper::VectorSetReader>& p_reader);

        public:
            bool AllFinished() { if (m_options.m_useKV || m_options.m_useSPDK) return m_extraSearcher->AllFinished(); return true; }

            void GetDBStat() { 
                if (m_options.m_useKV || m_options.m_useSPDK) m_extraSearcher->GetDBStats(); 
                LOG(Helper::LogLevel::LL_Info, "Current Vector Num: %d, Deleted: %d .\n", GetNumSamples(), GetNumDeleted());
            }

            void GetIndexStat(int finishedInsert, bool cost, bool reset) { if (m_options.m_useKV || m_options.m_useSPDK) m_extraSearcher->GetIndexStats(finishedInsert, cost, reset); }
            
            void ForceCompaction() { if (m_options.m_useKV) m_extraSearcher->ForceCompaction(); }

            void StopMerge() { m_options.m_inPlace = true; }

            void OpenMerge() { m_options.m_inPlace = false; }

            void ForceGC() { m_extraSearcher->ForceGC(m_index.get()); }

            bool Initialize() { return m_extraSearcher->Initialize(); }

            bool ExitBlockController() { return m_extraSearcher->ExitBlockController(); }

            ErrorCode AddIndexSPFresh(const void *p_data, SizeType p_vectorNum, DimensionType p_dimension, SizeType* VID) {
                if ((!m_options.m_useKV &&!m_options.m_useSPDK) || m_extraSearcher == nullptr) {
                    LOG(Helper::LogLevel::LL_Error, "Only Support KV Extra Update\n");
                    return ErrorCode::Fail;
                }

                if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0) return ErrorCode::EmptyData;
                if (p_dimension != GetFeatureDim()) return ErrorCode::DimensionSizeMismatch;

                SizeType begin, end;
                {
                    std::lock_guard<std::mutex> lock(m_dataAddLock);

                    begin = m_versionMap.GetVectorNum();
                    end = begin + p_vectorNum;

                    if (begin == 0) { return ErrorCode::EmptyIndex; }

                    if (m_versionMap.AddBatch(p_vectorNum) != ErrorCode::Success) {
                        LOG(Helper::LogLevel::LL_Info, "MemoryOverFlow: VID: %d, Map Size:%d\n", begin, m_versionMap.BufferSize());
                        exit(1);
                    }
                }
                for (int i = 0; i < p_vectorNum; i++) VID[i] = begin + i;

                std::shared_ptr<VectorSet> vectorSet;
                if (m_options.m_distCalcMethod == DistCalcMethod::Cosine) {
                    ByteArray arr = ByteArray::Alloc(sizeof(T) * p_vectorNum * p_dimension);
                    memcpy(arr.Data(), p_data, sizeof(T) * p_vectorNum * p_dimension);
                    vectorSet.reset(new BasicVectorSet(arr, GetEnumValueType<T>(), p_dimension, p_vectorNum));
                    int base = COMMON::Utils::GetBase<T>();
                    for (SizeType i = 0; i < p_vectorNum; i++) {
                        COMMON::Utils::Normalize((T*)(vectorSet->GetVector(i)), p_dimension, base);
                    }
                }
                else {
                    vectorSet.reset(new BasicVectorSet(ByteArray((std::uint8_t*)p_data, sizeof(T) * p_vectorNum * p_dimension, false),
                        GetEnumValueType<T>(), p_dimension, p_vectorNum));
                }

                return m_extraSearcher->AddIndex(vectorSet, m_index, begin);
            }

            ErrorCode SearchIndexRemote(QueryResult &p_query, SearchStats* p_stats)
            {
                if (!m_isCoordinator) {
                    LOG(Helper::LogLevel::LL_Info, "not Coordinator, can't not search!\n");
                    return ErrorCode::EmptyIndex;
                }

                COMMON::QueryResultSet<T>* p_queryResults;
                if (p_query.GetResultNum() >= m_options.m_searchInternalResultNum)
                    p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;
                else
                    p_queryResults = new COMMON::QueryResultSet<T>((const T*)p_query.GetTarget(), m_options.m_searchInternalResultNum);

                m_index->SearchIndex(*p_queryResults);


                if (m_workspace.get() == nullptr) {
                    m_workspace.reset(new ExtraWorkSpace());
                    m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                }
                m_workspace->m_deduper.clear();
                m_workspace->m_postingIDs.clear();

                std::vector<int> m_readedHead;

                for (int i = 0; i < p_queryResults->GetResultNum(); ++i)
                {
                    auto res = p_queryResults->GetResult(i);
                    if (res->VID == -1) break;
                    
                    auto postingID = res->VID;
                    if (m_vectorTranslateMap.get() != nullptr) res->VID = static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
                    else {
                        res->VID = -1;
                        res->Dist = MaxDist;
                    }

                    m_workspace->m_postingIDs.emplace_back(postingID);
                    if (m_vectorTranslateMap.get() != nullptr) m_readedHead.emplace_back(res->VID);
                }

                if (m_vectorTranslateMap.get() != nullptr) p_queryResults->Reverse();



                /***Remote Transefer And Process**/
                auto t3 = std::chrono::high_resolution_clock::now();
                int socket_fd = ClientConnect();
                RemoteQueryProcess(socket_fd, *p_queryResults, m_workspace->m_postingIDs, m_readedHead,  p_stats);
                CloseConnect(socket_fd);
                auto t4 = std::chrono::high_resolution_clock::now();

                double remoteProcessTime = std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count();

                p_stats->m_exLatency = remoteProcessTime;

                p_queryResults->SortResult();

                if (p_query.GetResultNum() < m_options.m_searchInternalResultNum) {
                    std::copy(p_queryResults->GetResults(), p_queryResults->GetResults() + p_query.GetResultNum(), p_query.GetResults());
                    delete p_queryResults;
                }

                if (p_query.WithMeta() && nullptr != m_pMetadata)
                {
                    for (int i = 0; i < p_query.GetResultNum(); ++i)
                    {
                        SizeType result = p_query.GetResult(i)->VID;
                        p_query.SetMetadata(i, (result < 0) ? ByteArray::c_empty : m_pMetadata->GetMetadataCopy(result));
                    }
                }
                return ErrorCode::Success;
            }

            inline void Serialize(char* ptr, QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead) {
                memcpy(ptr, p_queryResults.GetQuantizedTarget(), sizeof(T) * m_options.m_dim);
                int postingNum = m_postingIDs.size();
                memcpy(ptr + sizeof(T) * m_options.m_dim, &postingNum, sizeof(int));
                memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int), m_postingIDs.data(), sizeof(int) * postingNum);
                if (m_readedHead.size()!=0) {
                    memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int) * (postingNum + 1), m_readedHead.data(), sizeof(int) * postingNum);
                } 
            }

            ErrorCode RemoteQueryProcess(int socket_fd, QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead, SearchStats* p_stats) 
            {
                /** Serialize target vector, to be searched postings, allready readed VID**/
                if (m_options.m_remoteCalculation) {
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) + searched head VIDs**/
                    // Serialize(ptr, p_queryResults, m_postingIDs, m_readedHead);

                } else {
                    
                    int msgLength = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(int) * m_postingIDs.size();
                    std::string postingList(1 * msgLength, '\0');
                    char* ptr = (char*)(postingList.c_str());
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) **/
                    std::vector<int> m_readedHead_temp;
                    Serialize(ptr , p_queryResults, m_postingIDs, m_readedHead_temp);
                    write(socket_fd, ptr, msgLength);
                    int totalRead = 0;
                    /**First read total size**/
                    char msg_int[4];
                    while (totalRead < sizeof(int)) {
                        totalRead += read(socket_fd, msg_int + totalRead, sizeof(int) - totalRead);
                    }
                    int totalMsg_size = (*(int *)msg_int);
                    postingList.resize(totalMsg_size);
                    ptr = (char*)(postingList.c_str());
                    totalRead = 0;
                    while (totalRead < totalMsg_size) {
                        totalRead += read(socket_fd, ptr + totalRead, totalMsg_size - totalRead);
                    }

                    char msg_double[8];

                    totalRead = 0;
                    while (totalRead < sizeof(double)) {
                        totalRead += read(socket_fd, msg_double+ totalRead, sizeof(double) - totalRead);
                    }

                    p_stats->m_diskReadLatency = (*(double *)msg_double);

                    auto t1 = std::chrono::high_resolution_clock::now();

                    /**Process Vectors**/
                    ProcessPostingDSPANN(p_queryResults, postingList, m_readedHead);

                    auto t2 = std::chrono::high_resolution_clock::now();

                    double localProcessTime = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

                    p_stats->m_compLatency = localProcessTime;

                }
                return ErrorCode::Success;
            }

            void ProcessPostingDSPANN(QueryResult& p_queryResults, 
                std::string& postingList, std::vector<int>& m_readedHead)
            {
                int m_vectorInfoSize = sizeof(T) + sizeof(int) + sizeof(uint8_t);

                int m_metaDataSize = sizeof(int) + sizeof(uint8_t);

                int vectorNum = (int)(postingList.size() / m_vectorInfoSize);

                COMMON::QueryResultSet<T>& queryResults = *((COMMON::QueryResultSet<T>*) & p_queryResults);

                // if (m_workspace.get() == nullptr) {
                //     m_workspace.reset(new ExtraWorkSpace());
                //     m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                // }
                // m_workspace->m_deduper.clear();

                for (auto headID : m_readedHead) {
                    m_workspace->m_deduper.CheckAndSet(headID);
                }

                for (int i = 0; i < vectorNum; i++) {
                    char* vectorInfo = postingList.data() + i * m_vectorInfoSize;
                    int vectorID = *(reinterpret_cast<int*>(vectorInfo));

                    if(m_workspace->m_deduper.CheckAndSet(vectorID)) {
                        continue;
                    }
                    auto distance2leaf = m_index->ComputeDistance(queryResults.GetQuantizedTarget(), vectorInfo + m_metaDataSize);
                    queryResults.AddPoint(vectorID, distance2leaf);
                }
            }

            int ClientConnect() {
                int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
                if (socket_fd < 0) {
                    LOG(Helper::LogLevel::LL_Info, "can't open socket\n");
                    exit(0);
                }
                sockaddr_in m_addr;
                m_addr.sin_family = AF_INET;
                m_addr.sin_addr.s_addr = inet_addr(m_options.m_ipAddr.c_str());
                m_addr.sin_port = htons(m_options.m_port);
                int res = connect(socket_fd, (struct sockaddr*)&m_addr, sizeof(m_addr));
                if (res == -1) {
                    LOG(Helper::LogLevel::LL_Info, "bind failure\n");
                    exit(-1);
                }
                return socket_fd;
            }
            
            ErrorCode CloseConnect(int socket_fd) {
                close(socket_fd);
                return ErrorCode::Success;
            }

            ErrorCode HandleClient(int accept_socket) {
                int msg_size = m_options.m_dim * sizeof(T);
                    
                char* vectorBuffer = new char[msg_size];

                int totalRead = 0;
                while (totalRead < msg_size) {
                    totalRead += read(accept_socket, vectorBuffer + totalRead, msg_size - totalRead);
                }
                if (m_options.m_remoteCalculation) {

                } else {
                    int postingNum;
                    totalRead = 0;
                    while (totalRead < sizeof(int)) {
                        totalRead += read(accept_socket, ((char*) &postingNum) + totalRead, sizeof(int) - totalRead);
                    }
                    //LOG(Helper::LogLevel::LL_Info, "Need to read %d postings\n", postingNum);
                    std::vector<int> postingIDs(postingNum);
                    totalRead = 0;
                    while (totalRead < sizeof(int) * postingNum) {
                        totalRead += read(accept_socket, postingIDs.data() + totalRead, sizeof(int) * postingNum - totalRead);
                    }

                    std::vector<std::string> postingLists;

                    auto t1 = std::chrono::high_resolution_clock::now();
                    m_extraSearcher->GetMultiPosting(postingIDs, &postingLists);
                    auto t2 = std::chrono::high_resolution_clock::now();
                    double diskReadTime = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

                    int totalSize = 0;
                    for (int i = 0; i < postingLists.size(); i++) {
                        totalSize += postingLists[i].size();
                    }

                    //LOG(Helper::LogLevel::LL_Info, "Send back total Size: %d\n", totalSize);
                    write(accept_socket, ((char*)&totalSize), sizeof(int));

                    for (int i = 0; i < postingLists.size(); i++) {
                        write(accept_socket, postingLists[i].data(), postingLists[i].size());
                    }

                    write(accept_socket, ((char*)&diskReadTime), sizeof(double));
                    
                }
                return ErrorCode::Success;
            }

            ErrorCode ServerSetupListen() {
                struct sockaddr_in serv_addr, cli_addr;
                socklen_t clilen;
                int accept_socket;
                int server_socket = socket(AF_INET, SOCK_STREAM, 0);
                if (server_socket < 0) {
                    LOG(Helper::LogLevel::LL_Info, "can't open socket\n");
                    return ErrorCode::Undefined;
                }
                bzero((char *)&serv_addr, sizeof(serv_addr));
                serv_addr.sin_family = AF_INET;
                serv_addr.sin_addr.s_addr = inet_addr(m_options.m_ipAddr.c_str());
                serv_addr.sin_port = htons(m_options.m_port);
                if (bind(server_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                    LOG(Helper::LogLevel::LL_Info, "can't binding\n");
                    return ErrorCode::Undefined;
                }
                listen(server_socket, 30);
                clilen = sizeof(cli_addr);
                while (true) {
                    // LOG(Helper::LogLevel::LL_Info, "Start Listening\n");
                    accept_socket = accept(server_socket, (struct sockaddr *)&cli_addr, &clilen);
                    if (accept_socket < 0) {
                        LOG(Helper::LogLevel::LL_Info, "can't accept\n");
                        return ErrorCode::Undefined;
                    }
                    auto* curJob = new ClientJob(accept_socket, this, nullptr);
                    m_clientThreadPool->add(curJob);
                    // LOG(Helper::LogLevel::LL_Info, "Accpet from client\n");
                }
                
                return ErrorCode::Success;
            }
        };
    } // namespace SPANN
} // namespace SPTAG

#endif // _SPTAG_SPANN_INDEX_H_
