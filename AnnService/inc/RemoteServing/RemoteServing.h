#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
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

        template <typename ValueType>
        void SearchSequential(SPANN::Index<ValueType>* p_index,
            int p_numThreads,
            std::vector<QueryResult>& p_results,
            std::vector<SPANN::SearchStats>& p_stats,
            int p_internalResultNum)
        {
            int numQueries = static_cast<int>(p_results.size());

            std::atomic_size_t queriesSent(0);

            std::vector<std::thread> threads;

            LOG(Helper::LogLevel::LL_Info, "Searching: numThread: %d, numQueries: %d.\n", p_numThreads, numQueries);

            auto Tstart = std::chrono::high_resolution_clock::now();

            for (int i = 0; i < p_numThreads; i++) { threads.emplace_back([&, i]()
                {
                    // Helper::SetThreadAffinity( ((i+1) * 4), threads[i], 0, 0);
                    // p_index->ClientConnect();
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
                            p_index->SearchIndexRemote(p_results[index], &(p_stats[index]));
                            auto t2 = std::chrono::high_resolution_clock::now();
                            double totalTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                            p_stats[index].m_totalLatency = totalTime / 1000;
                        }
                        else
                        {
                            // p_index->ClientClose();
                            return;
                        }
                    }
                });
            }
            for (auto& thread : threads) { thread.join(); }

            auto Tend = std::chrono::high_resolution_clock::now();

            double pastTime = std::chrono::duration_cast<std::chrono::microseconds>(Tend - Tstart).count() / 1000000;

            LOG(Helper::LogLevel::LL_Info,
            "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query count %u.\n",
            pastTime,
            numQueries / pastTime,
            static_cast<uint32_t>(numQueries));
        }

        template <typename ValueType>
        void SearchRemote(SPANN::Index<ValueType>* p_index)
        {
            SPANN::Options& p_opts = *(p_index->GetOptions());

            int numThreads = p_opts.m_searchThreadNum;
            int internalResultNum = p_opts.m_searchInternalResultNum;
            std::string truthFile = p_opts.m_truthPath;

            int K = p_opts.m_resultNum;
            int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;

            auto querySet = LoadQuerySet(p_opts);

            int numQueries = querySet->Count();

            std::vector<QueryResult> results(numQueries, QueryResult(NULL, max(K, internalResultNum), false));
            std::vector<SPANN::SearchStats> stats(numQueries);
            for (int i = 0; i < numQueries; ++i)
            {
                (*((COMMON::QueryResultSet<ValueType>*)&results[i])).SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)), p_index->m_pQuantizer);
                results[i].Reset();
            }


            LOG(Helper::LogLevel::LL_Info, "Start ANN Search...\n");

            SearchSequential(p_index, numThreads, results, stats, internalResultNum);

            LOG(Helper::LogLevel::LL_Info, "\nFinish ANN Search...\n");

            LOG(Helper::LogLevel::LL_Info, "\nLocal L-0 In-memory Latency Distribution:\n");
            PrintPercentiles<double, SPANN::SearchStats>(stats,
                [](const SPANN::SearchStats& ss) -> double
                {
                    return ss.m_headLatency;
                },
                "%.3lf");
            for (int i = 0; i < numQueries; ++i) {
                stats[i].m_totalListElementsCount = stats[i].m_layerCounts[0];
            }
            LOG(Helper::LogLevel::LL_Info, "\nLocal L-0 In-memory Vectors:\n");
            PrintPercentiles<double, SPANN::SearchStats>(stats,
                [](const SPANN::SearchStats& ss) -> double
                {
                    return ss.m_totalListElementsCount;
                },
                "%.3lf");

            for (int layer = 0; layer < p_opts.m_layers-1; layer++) {
                for (int i = 0; i < numQueries; ++i) {
                    stats[i].m_compLatency = stats[i].m_compLatencys[layer];
                    stats[i].m_diskReadLatency = stats[i].m_diskReadLatencys[layer];
                    stats[i].m_exLatency = stats[i].m_exLatencys[layer];
                    stats[i].m_totalListElementsCount = stats[i].m_layerCounts[layer+1];
                }
                LOG(Helper::LogLevel::LL_Info, "\nL-%d Comp Latency Distribution:\n", layer+1);
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_compLatency;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nL-%d Remote Disk Read Latency Distribution:\n", layer+1);
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_diskReadLatency;
                    },
                    "%.3lf");

                if (!p_opts.m_isLocal) {
                    LOG(Helper::LogLevel::LL_Info, "\nL-%d Remote Transfer Latency Distribution:\n", layer+1);
                    PrintPercentiles<double, SPANN::SearchStats>(stats,
                        [](const SPANN::SearchStats& ss) -> double
                        {
                            return ss.m_exLatency;
                        },
                        "%.3lf");
                } else {
                    LOG(Helper::LogLevel::LL_Info, "\nL-%d Latency Distribution:\n", layer+1);
                    PrintPercentiles<double, SPANN::SearchStats>(stats,
                        [](const SPANN::SearchStats& ss) -> double
                        {
                            return ss.m_exLatency;
                        },
                        "%.3lf");
                }
                LOG(Helper::LogLevel::LL_Info, "\nLocal L-%d In-memory Vectors:\n", layer+1);
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalListElementsCount;
                    },
                    "%.3lf");
            }

            LOG(Helper::LogLevel::LL_Info, "\nTotal Latency Distribution:\n");
            PrintPercentiles<double, SPANN::SearchStats>(stats,
                [](const SPANN::SearchStats& ss) -> double
                {
                    return ss.m_totalLatency;
                },
                "%.3lf");

            std::vector<std::set<SizeType>> truth;
            float recall = 0, MRR = 0;
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

                recall = COMMON::TruthSet::CalculateRecall<ValueType>((p_index->GetMemoryIndex()).get(), results, truth, K, truthK, querySet, nullptr, numQueries, nullptr, false, &MRR);
                LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
            }

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
                    if (!opts->m_isLocal && !opts->m_isCoordinator) { \
                        ((SPANN::Index<Type>*)index.get())->BrokerOn(); \
                    } else { \
                        SearchRemote((SPANN::Index<Type>*)index.get()); \
                    } \
                } \

            #include "inc/Core/DefinitionList.h"
            #undef DefineVectorValueType

            return 0;

        }
    }
}