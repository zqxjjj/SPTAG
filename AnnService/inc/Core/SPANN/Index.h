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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <ratio>
#include <shared_mutex>

#include <string>
#include <zmq.h>
#include <zmq.hpp>

#include <iostream>
#include <fstream>

namespace SPTAG
{

    namespace Helper
    {
        class IniReader;
    }

    namespace SPANN
    {
        class NetworkJob : public Helper::ThreadPool::Job
        {
        public:
            std::string* request;
            std::string* reply;
            int* in_flight;
            double* latency;
            std::chrono::high_resolution_clock::time_point enterPoint;
            NetworkJob(std::string* request, std::string* reply, int* in_flight, double* latency = nullptr)
                : request(request), reply(reply), in_flight(in_flight), latency(latency) {
                    enterPoint = std::chrono::high_resolution_clock::now();
                }
            ~NetworkJob() {}
            inline void exec(IAbortOperation* p_abort) override {
                *in_flight = 0;
            }
        };

        class NetworkThreadPool : public Helper::ThreadPool
        {
        public:
            std::string m_address;

            ~NetworkThreadPool() 
            {
                m_abort.SetAbort(true);
                m_cond.notify_all();
                for (auto&& t : m_threads) t.join();
                m_threads.clear();
            }


            bool checkRemoteAvaliable()
            {
                LOG(Helper::LogLevel::LL_Info,"Test to connect: %s\n", m_address.c_str());
                zmq::context_t context(1);
                zmq::socket_t * checkSocket = new zmq::socket_t (context, ZMQ_DEALER);
                checkSocket->connect(m_address.c_str());
                int linger = 0;
                checkSocket->setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
                int retries_left = 3;
                while (retries_left) {
                    std::string tempData = "HeartBeat";
                    zmq::message_t pingpong(tempData.data(), tempData.size());
                    checkSocket->send(pingpong);
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    bool expect_reply = true;
                    while (expect_reply) {
                        //  Poll socket for a reply, with timeout
                        zmq::pollitem_t items[] = {
                            { *checkSocket, 0, ZMQ_POLLIN, 0 } };
                        zmq::poll (&items[0], 1, 2500);
                        //  If we got a reply, process it
                        if (items[0].revents & ZMQ_POLLIN) {
                            //  We got a reply from the server, must match sequence
                            zmq::message_t pingpong_reply;
                            checkSocket->recv(pingpong_reply);
                            LOG(Helper::LogLevel::LL_Info,"Successfully recv from %s\n", m_address.c_str());
                            delete checkSocket;
                            return true;
                        }
                        else if (--retries_left == 0) {
                            LOG(Helper::LogLevel::LL_Info,"No response from server, Failed\n");
                            delete checkSocket;
                            return false;
                        }
                        else {
                            LOG(Helper::LogLevel::LL_Info,"No response from server, retrying...\n");
                            //  Old socket will be confused; close it and open a new one
                            delete checkSocket;
                            checkSocket = new zmq::socket_t (context, ZMQ_REQ);
                            //  Send request again, on new socket
                            checkSocket->connect(m_address.c_str());
                        }
                    }
                }
                delete checkSocket;
                return true;
            }

            bool getNoBlock(Job*& j)
            {
                std::unique_lock<std::mutex> lock(m_lock);
                if (m_jobs.empty()) return false;
                if (!m_abort.ShouldAbort()) {
                    j = m_jobs.front();
                    m_jobs.pop();
                }
                return !m_abort.ShouldAbort();
            }

            void initNetwork(int numberOfThreads, std::string& m_ipAddrFrontend) 
            {
                m_address = m_ipAddrFrontend;
                m_abort.SetAbort(false);
                for (int i = 0; i < numberOfThreads; i++)
                {
                    m_threads.emplace_back([this, m_ipAddrFrontend] {
                        int key = 0;
                        std::map<int, NetworkJob*> unfinished;
                        zmq::context_t context(1);
                        zmq::socket_t clientSocket(context, ZMQ_DEALER);
                        // zmq::socket_t clientSocket(context, ZMQ_REQ);
                        clientSocket.connect(m_ipAddrFrontend.c_str());
                        zmq::pollitem_t items[] = {
                            { clientSocket, 0, ZMQ_POLLIN, 0 } };
                        Job *j;
                        while (!m_abort.ShouldAbort())
                        {
                            try 
                            {
                                if (currentJobs == 0) {
                                    if (!get(j)) {
                                        break;
                                    }
                                    NetworkJob *nj = static_cast<NetworkJob*>(j);
                                    currentJobs++;
                                    zmq::message_t request((nj->request)->size() + sizeof(int));

                                    char* ptr = static_cast<char*>(request.data());

                                    memcpy(ptr, (char*)&key, sizeof(int));
                                    unfinished[key] = nj;
                                    key++;
                                    ptr += sizeof(int);
                                    memcpy(ptr, (nj->request)->data(), (nj->request)->size());

                                    // LOG(Helper::LogLevel::LL_Info,"Send key: %d, size: %d\n", key-1, request.size());
                                    
                                    clientSocket.send(request);
                                } else if (getNoBlock(j)) {
                                    NetworkJob *nj = static_cast<NetworkJob*>(j);
                                    currentJobs++;
                                    zmq::message_t request((nj->request)->size() + sizeof(int));

                                    char* ptr = static_cast<char*>(request.data());

                                    memcpy(ptr, (char*)&key, sizeof(int));
                                    unfinished[key] = nj;
                                    key++;
                                    ptr += sizeof(int);
                                    memcpy(ptr, (nj->request)->data(), (nj->request)->size());

                                    // LOG(Helper::LogLevel::LL_Info,"Send key: %d\n", key-1);
                                    
                                    clientSocket.send(request);
                                } else {
                                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                                }

                                zmq::poll(items, 1, 0);
                                if (items[0].revents & ZMQ_POLLIN) {
                                    // LOG(Helper::LogLevel::LL_Info,"Get Reply\n");
                                    zmq::message_t reply;
                                    clientSocket.recv(reply);
                                    // LOG(Helper::LogLevel::LL_Info,"reply size: %d\n", reply.size());
                                    char* ptr = static_cast<char*>(reply.data());
                                    int currentKey;
                                    memcpy((char*)&currentKey, ptr, sizeof(int));
                                    // LOG(Helper::LogLevel::LL_Info,"Receive key: %d\n", currentKey);
                                    ptr += sizeof(int);
                                    (unfinished[currentKey]->reply)->resize(reply.size()-sizeof(int));
                                    // LOG(Helper::LogLevel::LL_Info,"Ready to copy\n", currentKey);
                                    memcpy((unfinished[currentKey]->reply)->data(), ptr, reply.size()-sizeof(int));
                                    *(unfinished[currentKey]->in_flight) = 0;
                                    if (unfinished[currentKey]->latency) {
                                        auto endTime = std::chrono::high_resolution_clock::now();
                                        *(unfinished[currentKey]->latency) = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(endTime - unfinished[currentKey]->enterPoint).count())) / 1000;
                                    }
                                    delete unfinished[currentKey];
                                    unfinished.erase(currentKey);
                                    currentJobs--;
                                    // LOG(Helper::LogLevel::LL_Info,"Finish one job\n");
                                }
                            }
                            catch (std::exception& e) {
                                LOG(Helper::LogLevel::LL_Error, "ThreadPool: exception in %s %s\n", typeid(*j).name(), e.what());
                            }
                        }
                        LOG(Helper::LogLevel::LL_Info, "Exit ThreadPool\n");
                        clientSocket.close();
                        context.shutdown();
                        context.close();
                    });
                }
            }
        };

        template<typename T>
        class Index : public VectorIndex
        {
        private:
            std::shared_ptr<VectorIndex> m_index;
            std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
            std::unordered_map<std::string, std::string> m_headParameters;

            std::shared_ptr<IExtraSearcher> m_extraSearcher;

            std::vector<std::shared_ptr<IExtraSearcher>> m_extraSearchers;
            std::vector<std::shared_ptr<std::uint64_t>> m_vectorTranslateMaps;

            Options m_options;

            std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
            int m_iBaseSquare;

            std::mutex m_dataAddLock;
            COMMON::VersionLabel m_versionMap;

            // If not Coord, than bind some port
            bool m_isCoordinator;

            std::shared_ptr<NetworkThreadPool> m_clientThreadPool;

            std::vector<std::shared_ptr<NetworkThreadPool>> m_commSocketPool;

            // Distributed KV Coordinator : maintain the map that where all vectors are stored
            // Given a key, Coordinator hash it, the hash value tells Coordinator where to read the value

            // For static searcher, we only need to directly send the read request to the specific node without converting the key

        public:
            static thread_local std::shared_ptr<ExtraWorkSpace> m_workspace;
            static thread_local std::shared_ptr<zmq::socket_t> clientSocket;
            static thread_local zmq::context_t context; 

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
                if (!m_options.m_multiLayer) {
                    auto headfiles = m_index->GetIndexFiles();
                    for (auto file : *headfiles) {
                        files->push_back(m_options.m_headIndexFolder + FolderSep + file);
                    }
                    if (m_options.m_excludehead) files->push_back(m_options.m_headIDFile);
                }
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
                SearchStats* p_stats = nullptr, std::set<SizeType>* truth = nullptr, std::map<int, std::set<SizeType>>* found = nullptr) const;
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

            ErrorCode LoadIndexHierarchyData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
            ErrorCode LoadIndexHierarchyDataLocal(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
            ErrorCode LoadIndexHierarchyDataDistKV(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
            ErrorCode LoadIndexDataToSPDKDevice();

        public:
            uint64_t ReturnTrueId(uint64_t headID) {
                if (!m_options.m_multiLayer)
                    return (m_vectorTranslateMap.get())[headID];
                else
                    return (m_vectorTranslateMaps[0].get())[headID];
            }

            int ReturnPostingSize(SizeType headID) {
                if (!m_options.m_multiLayer)
                    return m_extraSearcher->GetPostingSize(headID);
                else
                    return m_extraSearchers[0]->GetPostingSize(headID);
            }

            //only support single layer now
            void ReadPosting(SizeType headID, std::string& posting) {
                if (!m_options.m_multiLayer)
                    m_extraSearcher->GetWritePosting(headID, posting);
                else
                    m_extraSearchers[0]->GetWritePosting(headID, posting);
            }

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

            // void InitDSPANNNetWork() {
            //     m_commSocketPool.resize(m_options.m_dspannIndexFileNum * 2);
            //     // m_commSocketPool.resize(m_options.m_dspannIndexFileNum);

            //     // each node gets a replica, 8000&8001 for the first 8002&80003 for the second
            //     // for shard i, m_commSocketPool[i*2] and m_commSocketPool[i*2+1] are all for processing
            //     int node = 4;
            //     for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
            //         std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
            //         addrPrefix += std::to_string(node);
            //         addrPrefix += ":8000";
            //         LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
            //         m_commSocketPool[i*2] = std::make_shared<NetworkThreadPool>();
            //         m_commSocketPool[i*2]->initNetwork(m_options.m_searchThreadNum/m_options.m_dspannIndexFileNum, addrPrefix);
            //         // m_commSocketPool[i] = std::make_shared<NetworkThreadPool>();
            //         // m_commSocketPool[i]->initNetwork(m_options.m_searchThreadNum, addrPrefix);

            //         addrPrefix = m_options.m_ipAddrFrontendDSPANN;
            //         addrPrefix += std::to_string(node);
            //         addrPrefix += ":8002";
            //         LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
            //         m_commSocketPool[i*2+1] = std::make_shared<NetworkThreadPool>();
            //         m_commSocketPool[i*2+1]->initNetwork(m_options.m_searchThreadNum/m_options.m_dspannIndexFileNum, addrPrefix);
            //     }
            // }

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
                if (!(m_options.m_distKV && m_options.m_isCoordinator)) {
                    LOG(Helper::LogLevel::LL_Error, "Not the head index part process\n");
                    exit(0);
                }

                p_stats->m_layerCounts.resize(m_options.m_layers);
                auto t1 = std::chrono::high_resolution_clock::now();
                m_index->SearchIndex(p_query, p_stats->m_layerCounts[0]);
                auto t2 = std::chrono::high_resolution_clock::now();

                p_stats->m_headLatency = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count())) / 1000;

                COMMON::QueryResultSet<T>* p_queryResults;

                p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;

                if (m_workspace.get() == nullptr) {
                    m_workspace.reset(new ExtraWorkSpace());
                    m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                }

                m_workspace->m_deduper.clear();

                //Convert to global vector id
                for (int i = 0; i < p_queryResults->GetResultNum(); ++i)
                {
                    auto res = p_queryResults->GetResult(i);
                    if (res->VID == -1) break;
                    // LOG(Helper::LogLevel::LL_Info, "Head VID : %lu, Dist : %f\n", res->VID, res->Dist);
                    res->VID = static_cast<SizeType>((m_vectorTranslateMaps[0].get())[res->VID]);
                    // here we have already dedup vectors in merge stage, head index layer gets no replicated vector
                    if (m_workspace->m_deduper.CheckAndSet(res->VID));
                    // LOG(Helper::LogLevel::LL_Info, "Head Translated Global VID : %lu, Dist : %f\n", res->VID, res->Dist);
                }

                p_stats->m_compLatencys.resize(m_options.m_layers-1);
                p_stats->m_diskReadLatencys.resize(m_options.m_layers-1);
                p_stats->m_exLatencys.resize(m_options.m_layers-1);
                p_stats->m_diskReadPages.resize(m_options.m_layers-1);
                p_stats->m_exWaitLatencys.resize(m_options.m_layers-1);
                p_stats->m_RemoteRemoteLatencys.resize(m_options.m_layers-1);
                p_stats->m_accessPartitionsPerNode.resize(m_options.m_layers-1);

                for (int layer = 0; layer < m_options.m_layers - 1; layer++) {
                    // QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                    // COMMON::QueryResultSet<T>* p_tempResult = (COMMON::QueryResultSet<T>*) & p_Result;

                    // m_workspace->m_deduper.clear();
                    m_workspace->m_postingIDs.clear();

                    // here we need the current top internalResultNum Global VID, it should set in p_queryResults
                    for (int i = 0; i < p_queryResults->GetResultNum(); ++i)
                    {
                        auto res = p_queryResults->GetResult(i);
                        if (res->VID == -1) break;
                        // LOG(Helper::LogLevel::LL_Error, "Layer %d, VectorID: %lld, dist: %f\n", layer, res->VID, res->Dist);
                        auto postingID = res->VID;
                        m_workspace->m_postingIDs.emplace_back(postingID);

                        // if(!m_workspace->m_deduper.CheckAndSet(res->VID)) {
                        //     p_tempResult->AddPoint(res->VID, res->Dist);
                        // } 
                        // res->VID = -1;
                        // res->Dist = MaxDist;
                    }

                    p_queryResults->Reverse();

                    //sending request to the distKV
                    //Process m_postingIDs

                    std::vector<std::vector<SizeType>> keys_eachNode(GroupNum());

                    for (auto key: m_workspace->m_postingIDs) {
                        // LOG(Helper::LogLevel::LL_Info, "Debug Layer %d: key %d\n", layer+1, key);
                        if (key < 0) {
                            LOG(Helper::LogLevel::LL_Info, "Toplayer: temporarily find a key < 0(%d), in layer %d, drop it, but it is a bug\n", key, layer+1);
                            continue;
                        }
                        int node = NodeHash(key, layer);
                        keys_eachNode[node].push_back(key);
                    }

                    p_stats->m_accessPartitionsPerNode[layer].resize(GroupNum());
                    std::string* request[GroupNum()];
                    std::string* reply[GroupNum()];
                    std::vector<int> in_flight(GroupNum(), 0); 
                    std::vector<int> visit(GroupNum(), 0);
                    std::vector<double> transferLatency(GroupNum(), 0);

                    //Send to all distKV
                    auto t3 = std::chrono::high_resolution_clock::now();
                    for (int i = 0; i < GroupNum(); i++) {
                        p_stats->m_accessPartitionsPerNode[layer][i] = keys_eachNode[i].size();
                        if (keys_eachNode[i].size() != 0) {
                            request[i] = new std::string();
                            request[i]->resize(sizeof(SizeType)*(keys_eachNode[i].size() + 1) + m_options.m_dim * sizeof(T) + sizeof(SizeType) + sizeof(char));
                            reply[i] = new std::string();
                            in_flight[i] = 1;

                            char* ptr = static_cast<char*>(request[i]->data());

                            int keys_size = keys_eachNode[i].size();

                            memcpy(ptr, (char*)&keys_size, sizeof(int));
                            
                            ptr += sizeof(int);

                            memcpy(ptr, (char*)keys_eachNode[i].data(), sizeof(SizeType)*keys_eachNode[i].size());

                            ptr += sizeof(SizeType)*keys_eachNode[i].size();

                            //here we just transfer the full query vector instead of quantized query vector
                            memcpy(ptr, (char*)p_queryResults->GetTarget(), m_options.m_dim * sizeof(T));

                            ptr += m_options.m_dim * sizeof(T);

                            memcpy(ptr, (char*)&layer, sizeof(int));

                            ptr += sizeof(int);

                            char code = 0;

                            memcpy(ptr, (char*)&code, sizeof(char));

                            auto* curJob = new NetworkJob(request[i], reply[i], &in_flight[i], &transferLatency[i]);

                            m_commSocketPool[i]->add(curJob);
                            
                        } else {
                            visit[i] = 1;
                        }
                    }

                    bool notReady = true;

                    while (notReady) {
                        for (int i = 0; i < GroupNum(); ++i) {
                            if (visit[i] == 0 && in_flight[i] == 0) {
                                visit[i] = 1;
                                auto ptr = static_cast<char*>(reply[i]->data());
                                int totalSize = reply[i]->size();
                                if (m_options.m_tranPost) {
                                    // LOG(Helper::LogLevel::LL_Error, "Reading Result\n");
                                    std::string postingList;
                                    int totalSize = reply[i]->size();
                                    int currentSize = sizeof(double);
                                    int m_vectorSize;
                                    memcpy(&m_vectorSize, ptr, sizeof(int));

                                    ptr += sizeof(int);
                                    currentSize += sizeof(int);

                                    while (currentSize < totalSize) {
                                        int postingSize;
                                        memcpy(&postingSize, ptr, sizeof(int));
                                        ptr += sizeof(int);
                                        // LOG(Helper::LogLevel::LL_Error, "Reading Rosting: %d\n", postingSize);
                                        postingList.resize(postingSize);
                                        memcpy((char*)postingList.data(), ptr, postingSize);

                                        ptr += postingSize;

                                        //compute posting data

                                        int vectorNum = postingSize / m_vectorSize;

                                        for (int j = 0; j < vectorNum; j++) {
                                            SizeType vectorID = *(reinterpret_cast<SizeType*>(postingList.data() + j * m_vectorSize));
                                            // if (vectorID == 444703) LOG(Helper::LogLevel::LL_Error, "VectorID: %lld\n", vectorID);
                                            // LOG(Helper::LogLevel::LL_Error, "VectorID: %lld\n", vectorID);
                                            if (vectorID < 0) LOG(Helper::LogLevel::LL_Error, "Toplayer: No Near-data Processing, Find one vector ID gets minus: %lld\n", vectorID);
                                            if (m_workspace->m_deduper.CheckAndSet(vectorID)) continue;
                                            auto distance2leaf = m_index->ComputeDistance(p_queryResults->GetQuantizedTarget(), postingList.data() + j * m_vectorSize + sizeof(SizeType));
                                            p_queryResults->AddPoint(vectorID, distance2leaf);
                                        }
                                        currentSize += postingSize;
                                        currentSize += sizeof(int);
                                    }
                                    // finally read the last process time
                                } else {
                                    int totalResultNum = (totalSize - sizeof(double)) / (sizeof(SizeType) + sizeof(float));
                                    for (int j = 0; j < totalResultNum; j++) {
                                        SizeType VID;
                                        float Dist;
                                        memcpy((char *)&VID, ptr, sizeof(SizeType));
                                        memcpy((char *)&Dist, ptr + sizeof(SizeType), sizeof(float));
                                        ptr += sizeof(SizeType);
                                        ptr += sizeof(float);
                                        if (VID == -1) break;
                                        if (m_workspace->m_deduper.CheckAndSet(VID)) continue;
                                        // LOG(Helper::LogLevel::LL_Info, "Layer : %d, VID : %lu, Dist : %f\n", layer+1, VID, Dist);
                                        p_queryResults->AddPoint(VID, Dist);
                                    }
                                }
                                double localProcessTime;
                                memcpy(&localProcessTime, ptr, sizeof(double));
                                // LOG(Helper::LogLevel::LL_Info, "Local Processing : %lf, Transfer : %lf\n", layer+1, localProcessTime, transferLatency[i]);
                                transferLatency[i] -= (localProcessTime / 1000);
                                delete reply[i];
                                delete request[i];
                            }
                        }
                        notReady = false;
                        for (int i = 0; i < GroupNum(); ++i) {
                            if (visit[i] != 1) notReady = true;
                        }
                        if (notReady) std::this_thread::sleep_for(std::chrono::microseconds(5));
                    }

                    auto t4 = std::chrono::high_resolution_clock::now();

                    p_stats->m_diskReadLatencys[layer] = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count())) / 1000;

                    //caculate avg communication latency

                    for (int i = 0; i < GroupNum(); i++) {
                        p_stats->m_RemoteRemoteLatencys[layer] += transferLatency[i];
                    }
                    p_stats->m_RemoteRemoteLatencys[layer] /= GroupNum();

                    // p_tempResult->SortResult();
                    // for (int i = 0; i < p_tempResult->GetResultNum(); ++i) {
                    //     auto res = p_tempResult->GetResult(i);
                    //     if (res->VID == -1) break;
                    //     p_queryResults->AddPoint(res->VID, res->Dist);
                    // }
                    p_queryResults->SortResult();
                }
                // exit(0);
                return ErrorCode::Success;
            }

            ErrorCode WorkerTopLayerNode() {
                std::string tempData = "HeartBeat";
                LOG(Helper::LogLevel::LL_Info, "Start Worker HeadIndex\n");
                zmq::context_t context(1);

                // zmq::socket_t responder(context, ZMQ_REP);
                zmq::socket_t responder(context, ZMQ_DEALER);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                int msg_size = m_options.m_dim * sizeof(T);
                        
                char* vectorBuffer = new char[msg_size];

                LOG(Helper::LogLevel::LL_Info,"Ready For Recv\n");

                while(1) {

                    zmq::message_t identity;
                    zmq::message_t reply;
                    responder.recv(&identity);
                    responder.recv(&reply);

                    // LOG(Helper::LogLevel::LL_Info,"R: size: %d\n", reply.size());
                    if (reply.size() == tempData.size()) {
                        //pingpong
                        zmq::message_t request(1);
                        responder.send(identity, ZMQ_SNDMORE);
                        responder.send(request);
                        continue;
                    } 

                    // char* iptr = static_cast<char*>(identity.data());

                    // LOG(Helper::LogLevel::LL_Info,"Recv, identity: %d\n", *((int*)(iptr+1)));

                    auto t1 = std::chrono::high_resolution_clock::now();

                    char* ptr = static_cast<char*>(reply.data());

                    int currentKey;

                    memcpy((char*)&currentKey, ptr, sizeof(int));

                    ptr+=sizeof(int);

                    memcpy(vectorBuffer, ptr, msg_size);
                    // copy vector

                    QueryResult result(NULL, m_options.m_searchInternalResultNum, false);

                    (*((COMMON::QueryResultSet<T>*)&result)).SetTarget(reinterpret_cast<T*>(vectorBuffer), m_pQuantizer);
                    result.Reset();

                    SearchStats stats;

                    SearchIndexRemote(result, &stats);

                    int K = m_options.m_resultNum;

                    size_t msgSize = K * (sizeof(SizeType) + sizeof(float))+ sizeof(double) * m_options.m_layers * 2 + sizeof(int);

                    if (m_options.m_moreStats) {
                        msgSize += sizeof(int) * (m_options.m_layers - 1) * GroupNum();
                        msgSize += sizeof(int);
                    }


                    zmq::message_t request(msgSize);
                    COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & result;

                    ptr = static_cast<char*>(request.data());
                    memcpy(ptr, (char *)&currentKey, sizeof(int));
                    ptr += sizeof(int);
                    for (int i = 0; i < K; i++) {
                        auto res = queryResults->GetResult(i);
                        // LOG(Helper::LogLevel::LL_Info, "Final VID : %lu, Dist : %f\n", res->VID, res->Dist);
                        memcpy(ptr, (char *)&res->VID, sizeof(SizeType));
                        memcpy(ptr+sizeof(SizeType), (char *)&res->Dist, sizeof(float));
                        ptr+=sizeof(SizeType);
                        ptr+=sizeof(float);
                    }
                    // exit(0);

                    auto t2 = std::chrono::high_resolution_clock::now();

                    double localProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                    
                    localProcessTime /= 1000;

                    memcpy(ptr, (char *)&localProcessTime, sizeof(double));

                    ptr += sizeof(double);

                    memcpy(ptr, (char *)&stats.m_headLatency, sizeof(double));

                    ptr += sizeof(double);

                    for (int layer = 0; layer < m_options.m_layers -1; layer++) {
                        memcpy(ptr, (char *)&stats.m_diskReadLatencys[layer], sizeof(double));
                        ptr += sizeof(double);
                    }

                    for (int layer = 0; layer < m_options.m_layers -1; layer++) {
                        memcpy(ptr, (char *)&stats.m_RemoteRemoteLatencys[layer], sizeof(double));
                        ptr += sizeof(double);
                    }

                    if (m_options.m_moreStats) {
                        int totalStatNum = (m_options.m_layers - 1) * GroupNum();
                        memcpy(ptr, (char *)&totalStatNum, sizeof(int));
                        ptr += sizeof(int);
                        for (int layer = 0; layer < m_options.m_layers - 1; layer++) {
                            for (int node = 0; node < GroupNum(); node++) {
                                memcpy(ptr, (char *)&stats.m_accessPartitionsPerNode[layer][node], sizeof(int));
                                ptr += sizeof(int);
                            }
                        }
                    }


                    // LOG(Helper::LogLevel::LL_Info, "Send Back, size: %d, current key: %d\n", request.size(), currentKey);

                    // responder.send(request);
                    responder.send(identity, ZMQ_SNDMORE);
                    responder.send(request);
                }
                return ErrorCode::Success;
            }

            void initDistKVNetWork() {
                m_commSocketPool.resize(m_options.m_dspannIndexFileNum);
                // int node = 0;
                // for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                    // std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                    // addrPrefix += std::to_string(node + 4);
                    // addrPrefix += ":8000";
                    // LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                    // m_commSocketPool[i] = std::make_shared<NetworkThreadPool>();
                    // m_commSocketPool[i]->initNetwork(m_options.m_socketThreadNum, addrPrefix);
                // }
                // Debug version
                // int node = 0;
                // for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                //     std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                //     addrPrefix += std::to_string(node * 2);
                //     LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                //     m_commSocketPool[i] = std::make_shared<NetworkThreadPool>();
                //     m_commSocketPool[i]->initNetwork(m_options.m_socketThreadNum, addrPrefix);
                // }
                std::ifstream file(m_options.m_diskKVNetConfigPath); 
                std::string ipAddr;
                int count = 0;

                if (file.is_open()) {
                    while (std::getline(file, ipAddr)) {
                        LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", ipAddr.c_str());
                        m_commSocketPool[count] = std::make_shared<NetworkThreadPool>();
                        m_commSocketPool[count]->initNetwork(m_options.m_socketThreadNum, ipAddr);
                        count++;
                    }
                    file.close();
                } else {
                    LOG(Helper::LogLevel::LL_Info, "Can not open diskKV Network Config FIle: %s\n", m_options.m_diskKVNetConfigPath.c_str());
                }
                if (count < m_options.m_dspannIndexFileNum) {
                    LOG(Helper::LogLevel::LL_Info, "Can not get enough ip address, exit\n");
                    exit(0);
                }
            }

            void testDistKV() {
                LOG(Helper::LogLevel::LL_Info, "Begin with test distKV avaliable\n");
                if (m_options.m_isCoordinator)
                    for (int i = 0; i < m_options.m_dspannIndexFileNum; i++) {
                        if(!m_commSocketPool[i]->checkRemoteAvaliable()) {
                            exit(0);
                        }
                    }
            }

            int NodeHash(SizeType key, int layer) {
                return key % m_options.m_dspannIndexFileNum;
            }

            int MyNodeId() {
                return m_options.m_myNodeId;
            }

            int GroupNum() {
                return m_options.m_dspannIndexFileNum;
            }

            ErrorCode WorkerSingleIndex() {
                std::string tempData = "HeartBeat";
                // Warning: this code must be run under 4-Byte SizeType
                LOG(Helper::LogLevel::LL_Info, "Start Worker SingleIndex For Production Baseline\n");
                zmq::context_t context(1);

                // zmq::socket_t responder(context, ZMQ_REP);
                zmq::socket_t responder(context, ZMQ_DEALER);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                int msg_size = m_options.m_dim * sizeof(T);
                        
                char* vectorBuffer = new char[msg_size];

                LOG(Helper::LogLevel::LL_Info,"Ready For Recv\n");

                while(1) {

                    zmq::message_t identity;
                    zmq::message_t reply;
                    responder.recv(&identity);
                    responder.recv(&reply);

                    if(reply.size() == tempData.size()) {
                        //it is pingpong
                        zmq::message_t request(1);
                        responder.send(identity, ZMQ_SNDMORE);
                        responder.send(request);
                        continue;
                    }

                    // char* iptr = static_cast<char*>(identity.data());

                    // LOG(Helper::LogLevel::LL_Info,"Recv, identity: %d\n", *((int*)(iptr+1)));

                    auto t1 = std::chrono::high_resolution_clock::now();

                    char* ptr = static_cast<char*>(reply.data());

                    int currentKey;

                    memcpy((char*)&currentKey, ptr, sizeof(int));

                    ptr+=sizeof(int);

                    memcpy(vectorBuffer, ptr, msg_size);
                    // copy vector

                    QueryResult result(NULL, m_options.m_searchInternalResultNum, false);

                    (*((COMMON::QueryResultSet<T>*)&result)).SetTarget(reinterpret_cast<T*>(vectorBuffer), m_pQuantizer);
                    result.Reset();

                    SearchStats stats;

                    SearchIndex(result, &stats);

                    int K = m_options.m_resultNum;
                        
                    zmq::message_t request(K * (sizeof(std::uint64_t) + sizeof(float)) + sizeof(double) + sizeof(int));
                    COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & result;

                    ptr = static_cast<char*>(request.data());
                    memcpy(ptr, (char *)&currentKey, sizeof(int));
                    ptr += sizeof(int);
                    for (int i = 0; i < K; i++) {
                        auto res = queryResults->GetResult(i);
                        // LOG(Helper::LogLevel::LL_Info, "Final VID : %lu, Dist : %f\n", res->VID, res->Dist);
                        ByteArray globalVectorID_byteArray = GetMetadata(res->VID);
                        std::string globalVectorID_string;
                        globalVectorID_string.resize(globalVectorID_byteArray.Length());
                        memcpy((char*)globalVectorID_string.data(), (char*)globalVectorID_byteArray.Data(), globalVectorID_byteArray.Length());
                        std::uint64_t globalVectorID = std::stoull(globalVectorID_string);
                        memcpy(ptr, (char *)&globalVectorID, sizeof(std::uint64_t));
                        memcpy(ptr+sizeof(std::uint64_t), (char *)&res->Dist, sizeof(float));
                        ptr+=sizeof(std::uint64_t);
                        ptr+=sizeof(float);
                    }
                    // exit(0);

                    auto t2 = std::chrono::high_resolution_clock::now();

                    double localProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                    
                    localProcessTime /= 1000;

                    memcpy(ptr, (char *)&localProcessTime, sizeof(double));

                    ptr += sizeof(double);

                    // LOG(Helper::LogLevel::LL_Info, "Send Back, size: %d, current key: %d\n", request.size(), currentKey);

                    // responder.send(request);
                    responder.send(identity, ZMQ_SNDMORE);
                    responder.send(request);
                }
            }

            ErrorCode WorkerDistKV() {
                std::string tempData = "HeartBeat";
                LOG(Helper::LogLevel::LL_Info, "Start Worker DistKV\n");
                zmq::context_t context(1);

                // zmq::socket_t responder(context, ZMQ_REP);
                zmq::socket_t responder(context, ZMQ_DEALER);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                LOG(Helper::LogLevel::LL_Info,"Ready For Recv\n");

                char* vectorBuffer = new char[m_options.m_dim * sizeof(T)];

                while(1) {
                    QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                    COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & p_Result;
                    zmq::message_t reply;
                    zmq::message_t identity;
                    responder.recv(&identity);
                    responder.recv(&reply);

                    if (reply.size() == tempData.size()) {
                        //pingpong
                        zmq::message_t request(1);
                        responder.send(identity, ZMQ_SNDMORE);
                        responder.send(request);
                        continue;
                    } 

                    auto t1 = std::chrono::high_resolution_clock::now();
                    // client request, a list of key and vector and layer
                    // worker request, a list of key and vector and and char "0"
                    int size;
                    char* ptr = static_cast<char*>(reply.data());

                    int currentKey;
                    memcpy((char *)&currentKey, ptr, sizeof(int));

                    char* iptr = static_cast<char*>(identity.data());

                    // LOG(Helper::LogLevel::LL_Info, "KV Receive, key: %d, identity: %d\n", currentKey, *((int*)(iptr+1)));

                    ptr+=sizeof(int);
                    memcpy((char *)&size, ptr, sizeof(int));
                    ptr += sizeof(int);
                    std::vector<SizeType> keys(size);
                    memcpy((char *)keys.data(), ptr, sizeof(SizeType)*size);
                    ptr += sizeof(SizeType)*size;

                    memcpy(vectorBuffer, ptr, m_options.m_dim * sizeof(T));

                    ptr += m_options.m_dim * sizeof(T);

                    int layer;

                    memcpy((char *)&layer, ptr, sizeof(int));

                    if ((((size+2) * sizeof(SizeType)) + m_options.m_dim * sizeof(T) + 1 + sizeof(int)) == reply.size()) {
                        // worker request
                        auto t1 = std::chrono::high_resolution_clock::now();
                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }
                        m_workspace->m_deduper.clear();
                        m_workspace->m_postingIDs.clear();
                        // currently we exclude head from extraSearcher, so we do not need to add head information into m_deduper
                        for (auto key: keys) {
                            if (m_extraSearchers[layer]->GetPostingSize(key) != 0) m_workspace->m_postingIDs.push_back(key);
                        }
                        (*((COMMON::QueryResultSet<T>*)&p_Result)).SetTarget(reinterpret_cast<T*>(vectorBuffer), m_pQuantizer);
                        double compLatency = 0;
                        int scannedNum = 0;
                        VectorIndex* tempIndex = this;

                        if (!m_options.m_tranPost) {
                            m_extraSearchers[layer]->GetAndCompMultiPosting(m_workspace.get(), p_Result, tempIndex, compLatency, scannedNum, m_options);

                            // Return result
                            queryResults->SortResult();

                            int actualVectorNum = 0;
                            for (int i = 0; i < m_options.m_searchInternalResultNum; i++) {
                                auto res = queryResults->GetResult(i);
                                if (res->VID == -1) break;
                                actualVectorNum += 1;
                            }

                            // int K = m_options.m_searchInternalResultNum;
                            
                            zmq::message_t request(actualVectorNum * (sizeof(SizeType) + sizeof(float)) + sizeof(double) + sizeof(int));

                            ptr = static_cast<char*>(request.data());

                            memcpy(ptr, (char *)&currentKey, sizeof(int));

                            ptr += sizeof(int);

                            for (int i = 0; i < actualVectorNum; i++) {
                                auto res = queryResults->GetResult(i);
                                if (res->VID == -1) break;
                                // LOG(Helper::LogLevel::LL_Info, "Send Back, VID: %lu, Dist: %f\n", res->VID, res->Dist);
                                if (res->VID < -1) {
                                    LOG(Helper::LogLevel::LL_Info, "DistKV, temporarily find a wrong VID: %d when searching layer: %d, drop it but it is a bug\n", res->VID, layer+1);
                                    continue;
                                }
                                memcpy(ptr, (char *)&res->VID, sizeof(SizeType));
                                memcpy(ptr+sizeof(SizeType), (char *)&res->Dist, sizeof(float));
                                ptr+=sizeof(SizeType);
                                ptr+=sizeof(float);
                            }

                            auto t2 = std::chrono::high_resolution_clock::now();

                            double processTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));

                            memcpy(ptr, (char *)&processTime, sizeof(double));
                            // LOG(Helper::LogLevel::LL_Info, "Send Back, size: %d\n", request.size());

                            // responder.send(request);
                            responder.send(identity, ZMQ_SNDMORE);
                            responder.send(request);
                        } else {
                            std::vector<std::string> postingLists;
                            int vectorSize = 0;
                            m_extraSearchers[layer]->GetMultiPosting(m_workspace.get(), &postingLists, vectorSize);

                            int totalSize = sizeof(int);
                            totalSize += sizeof(int);
                            totalSize += sizeof(double);
                            for (int i = 0; i < postingLists.size(); i++) {
                                totalSize += sizeof(int);
                                totalSize += postingLists[i].size();
                            }

                            zmq::message_t request(totalSize);

                            ptr = static_cast<char*>(request.data());

                            memcpy(ptr, (char *)&currentKey, sizeof(int));

                            ptr += sizeof(int);

                            memcpy(ptr, (char *)&vectorSize, sizeof(int));

                            ptr += sizeof(int);

                            for (int i = 0; i < postingLists.size(); i++) {
                                int postingSize = postingLists[i].size();
                                memcpy(ptr, (char *)&postingSize, sizeof(int));
                                ptr += sizeof(int);
                                memcpy(ptr, (char *)postingLists[i].data(), postingSize);
                                ptr += postingSize;
                            }


                            auto t2 = std::chrono::high_resolution_clock::now();

                            //here is dist read latency
                            double processTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));

                            memcpy(ptr, (char *)&processTime, sizeof(double));

                            // LOG(Helper::LogLevel::LL_Info, "Send Back, size: %d\n", request.size());

                            // responder.send(request);
                            responder.send(identity, ZMQ_SNDMORE);
                            responder.send(request);
                        }

                    } else {
                        LOG(Helper::LogLevel::LL_Error, "Invalid msg: %d\n", reply.size());
                        exit(1);
                    }
                }
            }
        };
    } // namespace SPANN
} // namespace SPTAG

#endif // _SPTAG_SPANN_INDEX_H_
