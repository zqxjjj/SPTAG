#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/Utils.h"
#include <future>

#include <iomanip>
#include <iostream>
#include <fstream>
#include <ratio>

using namespace SPTAG;

namespace SPTAG {
	namespace RemoteServing {


        template<typename T, typename V>
        void PrintPercentiles(const std::vector<V>& p_values, std::function<T(const V&)> p_get, const char* p_format)
        {
            double sum = 0;
            std::vector<T> collects;
            collects.reserve(p_values.size());
            for (const auto& v : p_values)
            {
                T tmp = p_get(v);
                sum += tmp;
                collects.push_back(tmp);
            }

            std::sort(collects.begin(), collects.end());

            LOG(Helper::LogLevel::LL_Info, "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMax\n");

            std::string formatStr("%.3lf");
            for (int i = 1; i < 7; ++i)
            {
                formatStr += '\t';
                formatStr += p_format;
            }

            formatStr += '\n';

            LOG(Helper::LogLevel::LL_Info,
                formatStr.c_str(),
                sum / collects.size(),
                collects[static_cast<size_t>(collects.size() * 0.50)],
                collects[static_cast<size_t>(collects.size() * 0.90)],
                collects[static_cast<size_t>(collects.size() * 0.95)],
                collects[static_cast<size_t>(collects.size() * 0.99)],
                collects[static_cast<size_t>(collects.size() * 0.999)],
                collects[static_cast<size_t>(collects.size() - 1)]);
        }

        std::shared_ptr<VectorSet> LoadQuerySet(SPANN::Options& p_opts)
        {
            LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
            std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_queryType, p_opts.m_queryDelimiter));
            auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
            if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath))
            {
                LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                exit(1);
            }
            return queryReader->GetVectorSet();
        }

        void InitSPectrumNetWork(SPANN::Options& p_opts, std::vector<std::shared_ptr<SPANN::NetworkThreadPool>>& m_clientThreadPool) {
            m_clientThreadPool.resize(p_opts.m_dspannIndexFileNum);
            // int node = 4;
            // for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
            //     std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
            //     addrPrefix += std::to_string(node);
            //     addrPrefix += ":8002";
            //     LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
            //     m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
            //     m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_socketThreadNum, addrPrefix);
            // }

            // Debug version
            int node = 0;
            for (int i = 0; i < p_opts.m_dspannIndexFileNum; i++, node += 1) {
                std::string addrPrefix = p_opts.m_ipAddrFrontendDSPANN;
                addrPrefix += std::to_string(node*2);
                LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                m_clientThreadPool[i] = std::make_shared<SPANN::NetworkThreadPool>();
                m_clientThreadPool[i]->initNetwork(p_opts.m_socketThreadNum, addrPrefix);
            }
        }

        template <typename ValueType>
        ErrorCode SearchSPectrumRemote(SPANN::Options& p_opts, QueryResult &p_query, std::shared_ptr<SPANN::NetworkThreadPool> dispatchedNode, SPANN::SearchStats* stats) {
            COMMON::QueryResultSet<ValueType>* p_queryResults = (COMMON::QueryResultSet<ValueType>*) & p_query;
            std::string request;
            request.resize(p_opts.m_dim * sizeof(ValueType));
            std::string reply;
            int in_flight = 0;                
            memcpy(request.data(), (char*)p_query.GetTarget(), p_opts.m_dim * sizeof(ValueType));
            in_flight = 1;

            auto* curJob = new SPANN::NetworkJob(&request, &reply, &in_flight);

            dispatchedNode->add(curJob);

            while (in_flight != 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }

            auto ptr = static_cast<char*>(reply.data());
            for (int j = 0; j < p_opts.m_resultNum; j++) {
                SizeType VID;
                float Dist;
                memcpy((char *)&VID, ptr, sizeof(SizeType));
                memcpy((char *)&Dist, ptr + sizeof(SizeType), sizeof(float));
                ptr += sizeof(SizeType);
                ptr += sizeof(float);

                if (VID == -1) break;
                p_queryResults->AddPoint(VID, Dist);
            }
                        
            p_queryResults->SortResult();

            //record stats

            return ErrorCode::Success;
            
        }

        template <typename ValueType>
        void SPectrumSearchClient(SPANN::Index<ValueType>* p_index)
        {
            SPANN::Options& p_opts = *(p_index->GetOptions());

            std::vector<std::shared_ptr<SPANN::NetworkThreadPool>> m_clientThreadPool;

            InitSPectrumNetWork(p_opts, m_clientThreadPool);

            int internalResultNum = p_opts.m_searchInternalResultNum;
            auto querySet = LoadQuerySet(p_opts);

            int numQueries = querySet->Count();

            std::string truthFile = p_opts.m_truthPath;

            int K = p_opts.m_resultNum;
            int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;

            std::vector<QueryResult> results(numQueries, QueryResult(NULL, max(K, internalResultNum), false));
            std::vector<double> latency(numQueries);
            std::vector<SPANN::SearchStats> traverseLatency(numQueries);
            for (int i = 0; i < numQueries; ++i)
            {
                (*((COMMON::QueryResultSet<ValueType>*)&results[i])).SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)), p_index->m_pQuantizer);
                results[i].Reset();
            }
            LOG(Helper::LogLevel::LL_Info, "Load Query Finished\n");

            std::atomic_size_t queriesSent(0);

            std::vector<std::thread> threads;

            LOG(Helper::LogLevel::LL_Info, "Searching: numThread: %d, numQueries: %d.\n", p_opts.m_searchThreadNum, numQueries);

            SSDServing::Utils::StopW sw;

            for (int i = 0; i < p_opts.m_searchThreadNum; i++) { threads.emplace_back([&, i]()
                {
                    size_t index = 0;
                    while (true)
                    {
                        index = queriesSent.fetch_add(1);
                        if (index < numQueries)
                        {
                            if ((index & ((1 << 14) - 1)) == 0)
                            {
                                LOG(Helper::LogLevel::LL_Info, "Sent %.2lf%%...\n", index * 100.0 / numQueries);
                            }
                            auto t1 = std::chrono::high_resolution_clock::now();
                            SearchSPectrumRemote<ValueType>(p_opts, results[index], m_clientThreadPool[index % p_opts.m_dspannIndexFileNum], &traverseLatency[index]);
                            auto t2 = std::chrono::high_resolution_clock::now();
                            double totalTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                            latency[index] = totalTime / 1000;
                        }
                        else
                        {
                            return;
                        }
                    }
                });
            }
            for (auto& thread : threads) { thread.join(); }

            double sendingCost = sw.getElapsedSec();

            LOG(Helper::LogLevel::LL_Info,
                "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query count %u.\n",
                sendingCost,
                numQueries / sendingCost,
                static_cast<uint32_t>(numQueries));
            
            float recall = 0, MRR = 0;
            std::vector<std::set<SizeType>> truth;
            if (!truthFile.empty())
            {
                LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(truthFile.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthFile.c_str());
                    exit(1);
                }
                int originalK = truthK;
                COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK, p_opts.m_truthType);
                char tmp[4];
                if (ptr->ReadBinary(4, tmp) == 4) {
                    LOG(Helper::LogLevel::LL_Error, "Truth number is larger than query number(%d)!\n", numQueries);
                }

                recall = COMMON::TruthSet::CalculateRecall<ValueType>(p_index, results, truth, K, truthK, querySet, nullptr, numQueries, nullptr, false, &MRR);
                LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
            }

            LOG(Helper::LogLevel::LL_Info, "\nTotal Latency Distirbution:\n");
            PrintPercentiles<double, double>(latency,
                [](const double& ss) -> double
                {
                    return ss;
                },
                "%.3lf");
        }

        template <typename ValueType>
        ErrorCode BrokerOn(SPANN::Index<ValueType>* p_index) {
            LOG(Helper::LogLevel::LL_Info, "Start Broker\n");

            SPANN::Options& p_opts = *(p_index->GetOptions());

            zmq::context_t context(1);
            zmq::socket_t frontend (context, ZMQ_ROUTER);
            zmq::socket_t backend (context, ZMQ_DEALER);

            LOG(Helper::LogLevel::LL_Info, "Connecting Frontend: %s\n", p_opts.m_ipAddrFrontend.c_str());
            LOG(Helper::LogLevel::LL_Info, "Connecting Backend: %s\n", p_opts.m_ipAddrBackend.c_str());
            frontend.bind(p_opts.m_ipAddrFrontend.c_str());
            backend.bind(p_opts.m_ipAddrBackend.c_str());

            //  Initialize poll set
            zmq::pollitem_t items [] = {
                { frontend, 0, ZMQ_POLLIN, 0 },
                { backend, 0, ZMQ_POLLIN, 0 }
            };

            // TopLayer connect to all nodes
            if (p_opts.m_isCoordinator) p_index->initDistKVNetWork();

            std::vector<std::thread> m_threads;
            for (int i = 0; i < p_opts.m_searchThreadNum; i++)
            {
                m_threads.emplace_back([&] {
                    if (p_opts.m_distKV && !p_opts.m_isCoordinator)
                        p_index->WorkerDistKV();
                    else if (p_opts.m_distKV && p_opts.m_isCoordinator){
                        p_index->WorkerTopLayerNode();
                    } else {
                        LOG(Helper::LogLevel::LL_Info, "Unknown Process, Exit\n");
                        exit(0);
                    }
                });
            }

            try {
                zmq::proxy(static_cast<void*>(frontend),
                        static_cast<void*>(backend),
                        nullptr);
            }
            catch (std::exception &e) {}          
            return ErrorCode::Success;
        }

        int BootProgram(const char* storePath) {
            std::shared_ptr<VectorIndex> index;
            if (index->LoadIndex(storePath, index) != ErrorCode::Success) {
                LOG(Helper::LogLevel::LL_Error, "Failed to load index.\n");
                return 1;
            }

            SPANN::Options* opts = nullptr;

            #define DefineVectorValueType(Name, Type) \
                if (index->GetVectorValueType() == VectorValueType::Name) { \
                    opts = ((SPANN::Index<Type>*)index.get())->GetOptions(); \
                    if (true) { \
                        BrokerOn((SPANN::Index<Type>*)(index.get())); \
                    } else { \
                        LOG(Helper::LogLevel::LL_Error, "Error parameters.\n"); \
                    } \
                } \

            #include "inc/Core/DefinitionList.h"
            #undef DefineVectorValueType

            return 0;

        }
    }
}