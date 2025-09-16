#ifndef _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#define _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#define USE_ASYNC_IO

#include "inc/Helper/KeyValueIO.h"
#include "inc/Core/Common/Dataset.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/ThreadPool.h"
#include "inc/Helper/ConcurrentSet.h"
#include "inc/Helper/AsyncFileReader.h"
#include "inc/Core/SPANN/Options.h"
#include <cstdlib>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <list>
namespace SPTAG::SPANN {
    typedef std::int64_t AddressType;
    class FileIO : public Helper::KeyValueIO {
        class BlockController {
        private:
            char m_filePath[1024];
            std::shared_ptr <Helper::DiskIO> m_fileHandle = nullptr;

            Helper::Concurrent::ConcurrentQueue<AddressType> m_blockAddresses;
            Helper::Concurrent::ConcurrentQueue<AddressType> m_blockAddresses_reserve;

	        std::atomic<int64_t> read_complete_vec = 0;
	        std::atomic<int64_t> read_submit_vec = 0;
	        std::atomic<int64_t> write_complete_vec = 0;
	        std::atomic<int64_t> write_submit_vec = 0;
	        std::atomic<int64_t> read_bytes_vec = 0;
	        std::atomic<int64_t> write_bytes_vec = 0;
	        std::atomic<int64_t> read_blocks_time_vec = 0;

            float m_growthThreshold = 0.05;
            AddressType m_growthBlocks = 0;
            AddressType m_maxBlocks = 0;
            int m_batchSize = 64;
            int m_preIOCompleteCount = 0;
            int64_t m_preIOBytes = 0;
            bool m_disableCheckpoint = false;

            std::chrono::high_resolution_clock::time_point m_startTime;
            std::chrono::time_point<std::chrono::high_resolution_clock> m_preTime = std::chrono::high_resolution_clock::now();

            std::atomic<int64_t> m_batchReadTimes = 0;
            std::atomic<int64_t> m_batchReadTimeouts = 0;

            std::mutex m_expandLock;
            std::atomic<AddressType> m_totalAllocatedBlocks = 0;

        private:
            bool ExpandFile(AddressType blocksToAdd);
            bool NeedsExpansion(int psize);

            // static void Start(void* args);

            // static void FileIoLoop(void *arg);

            // static void FileIoCallback(bool success, void *cb_arg);

            // static void Stop(void* args);

        public:
            bool Initialize(SPANN::Options& p_opt);

            bool GetBlocks(AddressType* p_data, int p_size);

            bool ReleaseBlocks(AddressType* p_data, int p_size);

            bool ReadBlocks(AddressType* p_data, std::string* p_value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs);

            bool ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs);

            bool ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<Helper::PageBuffer<std::uint8_t>>& p_value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs);

            bool WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs);

            bool IOStatistics();

            bool ShutDown();

            int RemainBlocks() {
                return (int)(m_blockAddresses.unsafe_size());
            }

            int ReserveBlocks() {
                return (int)(m_blockAddresses_reserve.unsafe_size());
            }

            int TotalBlocks() {
                return (int)(m_totalAllocatedBlocks.load());
            }

            ErrorCode Checkpoint(std::string prefix) {
                std::string filename = prefix + "_blockpool";
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Checkpoint - Starting block pool save...\n");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Checkpoint - Reload reserved blocks...\n");
                AddressType currBlockAddress = 0;
                while (m_blockAddresses_reserve.try_pop(currBlockAddress))
                {
                    m_blockAddresses.push(currBlockAddress);
                }
                AddressType blocks = RemainBlocks();
                AddressType totalBlocks = m_totalAllocatedBlocks.load();

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Checkpoint - Total allocated blocks: %llu\n", static_cast<unsigned long long>(totalBlocks));
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Checkpoint - Remaining free blocks: %llu\n", blocks);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Checkpoint - Saving to file: %s\n", filename.c_str());

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::Checkpoint - Failed to open file: %s\n", filename.c_str());
                    return ErrorCode::FailedCreateFile;
                }
                IOBINARY(ptr, WriteBinary, sizeof(AddressType), reinterpret_cast<char*>(&blocks));
                IOBINARY(ptr, WriteBinary, sizeof(AddressType), reinterpret_cast<char*>(&totalBlocks));
                for (auto it = m_blockAddresses.unsafe_begin(); it != m_blockAddresses.unsafe_end(); it++) {
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType), reinterpret_cast<const char*>(&(*it)));
                }
		        /*
		        int i = 0;
                        for (auto it = m_blockAddresses.unsafe_begin(); it != m_blockAddresses.unsafe_end(); it++) {
		            std::cout << *it << " ";
		            i++;
		            if (i == 10) break;
		        }
		        std::cout << std::endl;
		        */
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save Finish!\n");
                return ErrorCode::Success;
            }

	        ErrorCode LoadBlockPool(std::string prefix, AddressType startNumBlocks, bool allowinit) {
	            std::string blockfile = prefix + "_blockpool";
                if (allowinit && !fileexists(blockfile.c_str())) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::LoadBlockPool: initializing fresh pool (no existing file found: %s)\n", blockfile.c_str());
                    for(AddressType i = 0; i < startNumBlocks; i++) {
                        m_blockAddresses.push(i);
                    }
                    m_totalAllocatedBlocks.store(startNumBlocks);

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::LoadBlockPool: initialized with %llu blocks (%.2f GB)\n", 
                        static_cast<unsigned long long>(startNumBlocks), static_cast<float>(startNumBlocks >> (30 - PageSizeEx)));
                } else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Load blockpool from %s\n", blockfile.c_str());
                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(blockfile.c_str(), std::ios::binary | std::ios::in)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot open the blockpool file: %s\n", blockfile.c_str());
    		            return ErrorCode::Fail;
                    }

                    AddressType blocks = 0;
                    AddressType totalAllocated = 0;

                    // Read block count
                    IOBINARY(ptr, ReadBinary, sizeof(AddressType), reinterpret_cast<char*>(&blocks));
                    IOBINARY(ptr, ReadBinary, sizeof(AddressType), reinterpret_cast<char*>(&totalAllocated));

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "FileIO::BlockController::LoadBlockPool: reading %llu free blocks into pool (%.2f GB), total allocated: %llu blocks\n",
                        static_cast<unsigned long long>(blocks),
                        static_cast<float>(blocks >> (30 - PageSizeEx)),
                        static_cast<unsigned long long>(totalAllocated));

                    AddressType currBlockAddress = 0;
                    for (AddressType i = 0; i < blocks; ++i) {
                        IOBINARY(ptr, ReadBinary, sizeof(AddressType), reinterpret_cast<char*>(&currBlockAddress));
                        m_blockAddresses.push(currBlockAddress);
                    }

                    m_totalAllocatedBlocks.store(totalAllocated);

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "FileIO::BlockController::LoadBlockPool: block pool initialized. Available: %llu, Total allocated: %llu\n",
                        static_cast<unsigned long long>(blocks),
                        static_cast<unsigned long long>(totalAllocated));
    	        }
		        /*
		        int i = 0;
                        for (auto it = m_blockAddresses.unsafe_begin(); it != m_blockAddresses.unsafe_end(); it++) {
		            std::cout << *it << " ";
		            i++;
		            if (i == 10) break;
		        }
		        std::cout << std::endl;
                */
                return ErrorCode::Success;
            } 
        };

        class LRUCache {
            int capacity;
            int limit;
            std::uint64_t size;
            std::list<SizeType> keys;  // Page Address
            Helper::Concurrent::ConcurrentMap<SizeType, std::pair<std::string, std::list<SizeType>::iterator>> cache;    // Page Address -> Page Address in Cache
            std::shared_timed_mutex mu;
            int64_t queries;
            std::atomic<int64_t> hits;
            FileIO* fileIO;
            std::vector<Helper::AsyncReadRequest> reqs;

        public:
            LRUCache(int capacity, int limit, FileIO* fileIO) {
                this->capacity = capacity;
                this->limit = min(capacity, (limit << PageSizeEx));
                this->size = 0;
                this->queries = 0;
                this->hits = 0;
                this->fileIO = fileIO;
                this->reqs.resize(limit);
            }

            bool evict(SizeType key, void* value, int vsize, std::unordered_map<SizeType, std::pair<std::string, std::list<SizeType>::iterator>>::iterator& it) {
                if (value != nullptr) {
                    std::string valstr((char*)value, vsize);
                    if (fileIO->Put(key, valstr, MaxTimeout, &reqs, false) != ErrorCode::Success) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "LRUCache: evict key:%d value size:%d to file failed\n", key, vsize);
                        return false;
                    }
                }

                size -= it->second.first.size();
                keys.erase(it->second.second);
                cache.erase(it);
                return true;
            }

            bool get(SizeType key, void* value) {
                std::shared_lock<std::shared_timed_mutex> lock(mu);
                queries++;
                auto it = cache.find(key);
                if (it == cache.end()) {
                    return false;  // If the key does not exist, return -1
                }
                // Update access order, move the key to the head of the linked list
                memcpy(value, it->second.first.data(), it->second.first.size());
                hits++;
                return true;
            }

            bool put(SizeType key, void* value, int put_size) {
                std::unique_lock<std::shared_timed_mutex> lock(mu);
                auto it = cache.find(key);
                if (it != cache.end()) {
                    if (put_size > limit) {
                        evict(key, it->second.first.data(), it->second.first.size(), it);
                        return false;
                    }
                    keys.splice(keys.begin(), keys, it->second.second);
                    it->second.second = keys.begin();

                    auto delta_size = put_size - it->second.first.size();
                    while ((capacity - size) < delta_size && (keys.size() > 1)) {
                        auto last = keys.back();
                        auto lastit = cache.find(last);
                        if (!evict(last, lastit->second.first.data(), lastit->second.first.size(), lastit)) {
                            return false;
                        }
                    }
                    it->second.first.resize(put_size);
                    memcpy(it->second.first.data(), value, put_size);
                    size += delta_size;
                    return true;
                }
                if (put_size > limit) {
                    return false;
                }
                while (put_size > (capacity - size) && (!keys.empty())) {
                    auto last = keys.back();
                    auto lastit = cache.find(last);
                    if (!evict(last, lastit->second.first.data(), lastit->second.first.size(), lastit)) {
                        return false;
                    }
                }
                auto keys_it = keys.insert(keys.begin(), key);
                cache.insert({key, {std::string((char*)value, put_size), keys_it}});
                size += put_size;
                return true;
            }

            bool del(SizeType key) {
                std::unique_lock<std::shared_timed_mutex> lock(mu);
                auto it = cache.find(key);
                if (it == cache.end()) {
                    return false; // If the key does not exist, return false
                }
                evict(key, nullptr, 0, it);
                return true;
            }

            bool merge(SizeType key, void* value, AddressType merge_size) {
                std::unique_lock<std::shared_timed_mutex> lock(mu);
                // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "LRUCache: merge size: %lld\n", merge_size);
                auto it = cache.find(key);
                if (it == cache.end()) {
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "LRUCache: merge key not found\n");
                    std::string valstr;
                    if (fileIO->Get(key, &valstr, MaxTimeout, &reqs, false) != ErrorCode::Success) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "LRUCache: merge key not found in file\n");
                        return false;  // If the key does not exist, return false
                    }
                    cache.insert({key, {valstr, keys.insert(keys.begin(), key)}});
                    size += valstr.size();
                    it = cache.find(key);
                }

                if (merge_size + it->second.first.size() > limit) {
                    evict(key, it->second.first.data(), it->second.first.size(), it);
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "LRUCache: merge size exceeded\n");
                    return false;
                }
                keys.splice(keys.begin(), keys, it->second.second);
                it->second.second = keys.begin();
                while((capacity - size) < merge_size && (keys.size() > 1)) {
                    auto last = keys.back();
                    auto lastit = cache.find(last);
                    if (!evict(last, lastit->second.first.data(), lastit->second.first.size(), lastit)) {
                        return false;
                    }
                }
                it->second.first.append((char*)value, merge_size);
                size += merge_size;
                // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "LRUCache: merge success\n");
                return true;
            }
            
            std::pair<int64_t, int64_t> get_stat() {
                return {queries, hits.load()};
            }

            bool flush() {
                std::unique_lock<std::shared_timed_mutex> lock(mu);
                for (auto it = cache.begin(); it != cache.end(); it++) {
                    if (fileIO->Put(it->first, it->second.first, MaxTimeout, &reqs, false) != ErrorCode::Success) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "LRUCache: evict key:%d value size:%d to file failed\n", it->first, (int)(it->second.first.size()));
                        return false;
                    }
                }
                cache.clear();
                keys.clear();
                size = 0;
                return true;
            }
        }; 

        class ShardedLRUCache {
            int shards;
            std::vector<LRUCache*> caches;
            SizeType hash(SizeType key) const {
                return key % shards;
            }
        public:
            ShardedLRUCache(int shards, int capacity, int limit, FileIO* fileIO) : shards(shards) {
                caches.resize(shards);
                for (int i = 0; i < shards; i++) {
                    caches[i] = new LRUCache(capacity / shards, limit, fileIO);
                }
                if (capacity % shards != 0) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, "LRUCache: capacity is not divisible by shards\n");
                }
            }

            ~ShardedLRUCache() {
                for (int i = 0; i < shards; i++) {
                    delete caches[i];
                }
            }

            bool get(SizeType key, void* value) {
                return caches[hash(key)]->get(key, value);
            }

            bool put(SizeType key, void* value, SizeType put_size) {
                return caches[hash(key)]->put(key, value, put_size);
            }

            bool del(SizeType key) {
                return caches[hash(key)]->del(key);
            }

            bool merge(SizeType key, void* value, AddressType merge_size) {
                return caches[hash(key)]->merge(key, value, merge_size);
            }

            bool flush() {
                for (int i = 0; i < shards; i++) {
                    if (!caches[i]->flush()) return false;
                }
                return true;
            }

            std::pair<int64_t, int64_t> get_stat() {
                int64_t queries = 0, hits = 0;
                for (int i = 0; i < shards; i++) {
                    auto stat = caches[i]->get_stat();
                    queries += stat.first;
                    hits += stat.second;
                }
                return {queries, hits};
            }
        };

    public:
        FileIO(SPANN::Options& p_opt) {
            m_mappingPath = p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdMappingFile;
            m_blockLimit = max(p_opt.m_postingPageLimit, p_opt.m_searchPostingPageLimit) + p_opt.m_bufferLength + 1;
            m_bufferLimit = 1024;
            m_shutdownCalled = true;

            const char* fileIoUseLock = getenv(kFileIoUseLock);
            if(fileIoUseLock) {
                if(strcmp(fileIoUseLock, "True") == 0) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: Using lock\n");
                    m_fileIoUseLock = true;
                    const char* fileIoLockSize = getenv(kFileIoLockSize);
                    if(fileIoLockSize) {
                        m_fileIoLockSize = atoi(fileIoLockSize);
                    }
                    m_rwMutex = std::vector<std::shared_mutex>(m_fileIoLockSize);
                }
                else {
                    m_fileIoUseLock = false;
                }
            }
            else {
                m_fileIoUseLock = false;
            }

            m_pShardedLRUCache = nullptr;
            if (p_opt.m_cacheSize > 0) {
                int capacity = p_opt.m_cacheSize << 30;
                m_pShardedLRUCache = new ShardedLRUCache(p_opt.m_cacheShards, capacity, m_blockLimit, this);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: Using LRU Cache with capacity %d GB, limit %d pages, shards %d\n", p_opt.m_cacheSize, m_blockLimit, p_opt.m_cacheShards);
            }

            if (p_opt.m_recovery) {
                std::string recoverpath = p_opt.m_persistentBufferPath + FolderSep + p_opt.m_ssdMappingFile;
                if (fileexists(recoverpath.c_str())) {
                    Load(recoverpath, 1024 * 1024, MaxSize);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load block mapping successfully from %s!\n", recoverpath.c_str());
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot recover block mapping from %s!\n", recoverpath.c_str());
                    return;
                }
            }
            else {
                if (fileexists(m_mappingPath.c_str())) {
                    Load(m_mappingPath, 1024 * 1024, MaxSize);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load block mapping successfully from %s!\n", m_mappingPath.c_str());
		    /*
                    for (int i = 0; i < 10; i++) {
                        std::cout << "i=" << i << ": ";
                        for (int j = 0; j < 10; j++) {
                            std::cout << m_pBlockMapping[i][j] << " ";
                        }
                        std::cout << std::endl;
                    }
                    for (int i = m_pBlockMapping.R() - 10; i < m_pBlockMapping.R(); i++) {
                        std::cout << "i=" << i << ": ";
                        for (int j = 0; j < 10; j++) {
                            std::cout << m_pBlockMapping[i][j] << " ";
                        }
                        std::cout << std::endl;
                    }
		    */
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Initialize block mapping successfully!\n");
                    m_pBlockMapping.Initialize(0, 1, 1024 * 1024, MaxSize);
                }
            }
            for (int i = 0; i < m_bufferLimit; i++) {
                m_buffer.push((uintptr_t)(new AddressType[m_blockLimit]));
            }
            m_compactionThreadPool = std::make_shared<Helper::ThreadPool>();
            m_compactionThreadPool->init(1);

            if (!m_pBlockController.Initialize(p_opt)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Initialize FileIO!\n");
                return;
            }

            m_shutdownCalled = false;
        }

        ~FileIO() {
            ShutDown();
        }

        bool Available() override
        {
            return !m_shutdownCalled;
        }

        void ShutDown() override {
            if (m_shutdownCalled) {
                return;
            }
            m_shutdownCalled = true;
            while (!m_key_reserve.empty())
            {
                SizeType cleanKey = 0xffffffff;
                if (m_key_reserve.try_pop(cleanKey))
                {
                    At(cleanKey) = 0xffffffffffffffff;
                }
            }
            if (!m_mappingPath.empty()) Save(m_mappingPath);
            // TODO: Should we add a lock here?
            for (int i = 0; i < m_pBlockMapping.R(); i++) {
                if (At(i) != 0xffffffffffffffff) {
                    //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "delete at %d for addr: %llu\n", i, At(i));
                    delete[]((AddressType*)At(i));
                    At(i) = 0xffffffffffffffff;
                }
            }
            while (!m_buffer.empty()) {
                uintptr_t ptr = 0xffffffffffffffff;
                if (m_buffer.try_pop(ptr)) delete[]((AddressType*)ptr);
            }
	        m_pBlockController.ShutDown();
            if (m_pShardedLRUCache) {
                delete m_pShardedLRUCache;
                m_pShardedLRUCache = nullptr;
            }
        }

        inline uintptr_t& At(SizeType key) {
            return *(m_pBlockMapping[key]);
        }
        
        ErrorCode Get(const SizeType key, std::string* value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs, bool useCache) {
            auto get_begin_time = std::chrono::high_resolution_clock::now();
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock_shared();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Key OverFlow! Key:%d R:%d\n", key, r);
                return ErrorCode::Key_OverFlow;
            }
            AddressType* addr = (AddressType*)(At(key));
            if (((uintptr_t)addr) == 0xffffffffffffffff) {
                 SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Key NotFound! Key:%d\n", key);
                return ErrorCode::Key_NotFound;
            }

            auto size = addr[0];
            if (size < 0) return ErrorCode::Posting_SizeError;

            if (useCache && m_pShardedLRUCache) {
                value->resize(size);
                if (m_pShardedLRUCache->get(key, value->data())) {
                    return ErrorCode::Success;
                }
            }
            
            // if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) {
            //     return ErrorCode::Success;
            // }
            auto begin_time = std::chrono::high_resolution_clock::now();
            auto result = m_pBlockController.ReadBlocks((AddressType*)At(key), value, timeout, reqs);
            auto end_time = std::chrono::high_resolution_clock::now();
            read_time_vec += std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
            get_times_vec++;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock_shared();
            }
            auto get_end_time = std::chrono::high_resolution_clock::now();
            get_time_vec += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode Get(const SizeType key, std::string* value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            return Get(key, value, timeout, reqs, true);
        }

        ErrorCode Get(const std::string& key, std::string* value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            return Get(std::stoi(key), value, timeout, reqs, true);
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<Helper::PageBuffer<std::uint8_t>>& values,
            const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            std::vector<AddressType*> blocks;
            std::set<int> lock_keys;
            if (m_fileIoUseLock) {
                // dedup？
                // for (SizeType key : keys) {
                //     lock_keys.insert(hash(key));
                // }
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].lock_shared();
                }
            }
            SizeType r;
            int i = 0;
            for (SizeType key : keys) {
                if (m_fileIoUseLock) {
                    m_updateMutex.lock_shared();
                    r = m_pBlockMapping.R();
                    m_updateMutex.unlock_shared();
                }
                else {
                    r = m_pBlockMapping.R();
                }
                if (key < r) {
                    AddressType* addr = (AddressType*)(At(key));
                    if (m_pShardedLRUCache  && ((uintptr_t)addr) != 0xffffffffffffffff && addr[0] >= 0) {
                        values[i].SetAvailableSize(addr[0]);
                        if (m_pShardedLRUCache->get(key, values[i].GetBuffer())) {
                            blocks.push_back(nullptr);
                        }
                        else {
                            blocks.push_back(addr);
                        }
                    } else {
                        blocks.push_back(addr);
                    }      
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to read key:%d total key number:%d\n", key, r);
                }
                i++;
            }
            // if (m_pBlockController.ReadBlocks(blocks, values, timeout)) return ErrorCode::Success;
            auto result = m_pBlockController.ReadBlocks(blocks, values, timeout, reqs);
            if (m_fileIoUseLock) {
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].unlock_shared();
                }
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }


        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values,
            const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
            std::vector<AddressType*> blocks;
            std::set<int> lock_keys;
            if (m_fileIoUseLock) {
                // dedup?
                // for (SizeType key : keys) {
                //     lock_keys.insert(hash(key));
                // }
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].lock_shared();
                }
            }
            SizeType r;
            values->resize(keys.size());
            int i = 0;
            for (SizeType key : keys) {
                if (m_fileIoUseLock) {
                    m_updateMutex.lock_shared();
                    r = m_pBlockMapping.R();
                    m_updateMutex.unlock_shared();
                }
                else {
                    r = m_pBlockMapping.R();
                }
                if (key < r) {
                    AddressType* addr = (AddressType*)(At(key));
                    if (m_pShardedLRUCache && ((uintptr_t)addr) != 0xffffffffffffffff && addr[0] >= 0) {   
                        (*values)[i].resize(addr[0]);
                        if (m_pShardedLRUCache->get(key, (*values)[i].data())) {
                            blocks.push_back(nullptr);
                        }
                        else {
                            blocks.push_back(addr);
                        }
                    }
                    else {
                        blocks.push_back(addr);
                    }
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to read key:%d total key number:%d\n", key, r);
                }
                i++;
            }
            // if (m_pBlockController.ReadBlocks(blocks, values, timeout)) return ErrorCode::Success;
            auto result = m_pBlockController.ReadBlocks(blocks, values, timeout, reqs);
            if (m_fileIoUseLock) {
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].unlock_shared();
                }
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode MultiGet(const std::vector<std::string>& keys, std::vector<std::string>* values, 
            const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            std::vector<SizeType> int_keys;
            for (const auto& key : keys) {
                int_keys.push_back(std::stoi(key));
            }
            return MultiGet(int_keys, values, timeout, reqs);
        }

        /*
        ErrorCode Scan(const SizeType start_key, const int record_count, std::vector<ByteArray> &values, const std::chrono::microseconds &timeout = (std::chrono::microseconds::max)(), std::vector<Helper::AsyncReadRequest>* reqs = nullptr) {
            std::vector<SizeType> keys;
            std::vector<AddressType*> blocks;
            SizeType curr_key = start_key;
            while(keys.size() < record_count && curr_key < m_pBlockMapping.R()) {
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(curr_key)].lock_shared();
                }
                if (At(curr_key) == 0xffffffffffffffff) {
                    if (m_fileIoUseLock) {
                        m_rwMutex[hash(curr_key)].unlock_shared();
                    }
                    curr_key++;
                    continue;
                }
                keys.push_back(curr_key);
                blocks.push_back((AddressType*)At(curr_key));
                curr_key++;
            }
            auto result = m_pBlockController.ReadBlocks(blocks, values, timeout, reqs);
            if (m_fileIoUseLock) {
                for (auto key : keys) {
                    m_rwMutex[hash(key)].unlock_shared();
                }
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }
        */

        ErrorCode Put(const SizeType key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs, bool useCache) {
            int blocks = (int)(((value.size() + PageSize - 1) >> PageSizeEx));
            if (blocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to put key:%d value:%lld since value too long!\n", key, value.size());
                return ErrorCode::Posting_OverFlow;
            }
            // Calculate whether more mapping blocks are needed
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            int delta;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                delta = key + 1 - m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                delta = key + 1 - m_pBlockMapping.R();
            }
            if (delta > 0) {
                // std::lock_guard<std::mutex> lock(m_updateMutex);
                m_updateMutex.lock();
                delta = key + 1 - m_pBlockMapping.R();
                if (delta > 0) {
                    m_pBlockMapping.AddBatch(delta);
                }
                m_updateMutex.unlock();
            }

            if (useCache && m_pShardedLRUCache) {
                if (m_pShardedLRUCache->put(key, (void*)(value.data()), (SPTAG::SizeType)(value.size()))) {
                    return ErrorCode::Success;
                }
            }

            // If this key has not been assigned mapping blocks yet, allocate a batch.
            if (At(key) == 0xffffffffffffffff) {
                // If there are spare blocks in m_buffer, use them directly; otherwise, allocate a new batch.
                if (m_buffer.unsafe_size() > m_bufferLimit) {
                    uintptr_t tmpblocks = 0xffffffffffffffff;
                    while (!m_buffer.try_pop(tmpblocks));
                    At(key) = tmpblocks;
                }
                else {
                    At(key) = (uintptr_t)(new AddressType[m_blockLimit]);
                }
                // The 0th element of the block address list represents the data size; set it to -1.
                memset((AddressType*)At(key), -1, sizeof(AddressType) * m_blockLimit);
            }
            int64_t* postingSize = (int64_t*)At(key);
            // If postingSize is less than 0, it means the mapping block is newly allocated—directly
            if (*postingSize < 0) {
                if (!m_pBlockController.GetBlocks(postingSize + 1, blocks))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "[Put] Not enough blocks in the pool can be allocated!\n");
                    return ErrorCode::DiskIOFail;
                }
                if (!m_pBlockController.WriteBlocks(postingSize + 1, blocks, value, timeout, reqs))
                {
                    m_pBlockController.ReleaseBlocks(postingSize + 1, blocks);
                    memset(postingSize + 1, -1, sizeof(AddressType) * blocks);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Put] Write new block failed!\n");
                    return ErrorCode::DiskIOFail;
                }
                *postingSize = value.size();
            }
            else {
                uintptr_t tmpblocks = 0xffffffffffffffff;
                // Take a batch of mapping blocks from the buffer, and return a batch back later.
                while (!m_buffer.try_pop(tmpblocks));
                // Acquire a new batch of disk blocks and write data directly.
                // To ensure the effectiveness of the checkpoint, new blocks must be allocated for writing here.
                if (!m_pBlockController.GetBlocks((AddressType*)tmpblocks + 1, blocks))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Put] Not enough blocks in the pool can be allocated!\n");
                    m_buffer.push(tmpblocks);
                    return ErrorCode::DiskIOFail;
                }
                if (!m_pBlockController.WriteBlocks((AddressType*)tmpblocks + 1, blocks, value, timeout, reqs))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Put] Write new block failed!\n");
                    m_pBlockController.ReleaseBlocks((AddressType*)tmpblocks + 1, blocks);
                    m_buffer.push(tmpblocks);
                    return ErrorCode::DiskIOFail;
                }
                *((int64_t*)tmpblocks) = value.size();

                // Release the original blocks
                m_pBlockController.ReleaseBlocks(postingSize + 1, (*postingSize + PageSize - 1) >> PageSizeEx);
                while (InterlockedCompareExchange(&At(key), tmpblocks, (uintptr_t)postingSize) != (uintptr_t)postingSize) {
                    postingSize = (int64_t*)At(key);
                }
                m_buffer.push((uintptr_t)postingSize);
            }
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Put(const SizeType key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            return Put(key, value, timeout, reqs, true);
        }

        ErrorCode Put(const std::string &key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) override {
            return Put(std::stoi(key), value, timeout, reqs, true);
        }

        ErrorCode Check(const SizeType key, int size) override
        {
            if (m_fileIoUseLock)
            {
                m_rwMutex[hash(key)].lock();
            }
            SizeType r;
            if (m_fileIoUseLock)
            {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else
            {
                r = m_pBlockMapping.R();
            }

            if (key >= r || At(key) == 0xffffffffffffffff)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Check] Key range error: key: %d, mapping size: %d\n", key, r);
                if (m_fileIoUseLock)
                {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Key_OverFlow;
            }

            int64_t *postingSize = (int64_t *)At(key);
            if ((*postingSize) != size)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Check] Key %d failed: postingSize %d is not match real size %d\n", key, (int)(*postingSize), size);
                if (m_fileIoUseLock)
                {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Posting_SizeError;
            }

            int blocks = ((*postingSize + PageSize - 1) >> PageSizeEx);
            std::vector<bool> checked(m_pBlockController.TotalBlocks(), false);
            for (int i = 1; i <= blocks; i++)
            {
                if (postingSize[i] < 0 || postingSize[i] >= m_pBlockController.TotalBlocks())
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "[Check] Key %d failed: error block id %d (should be 0 ~ %d)\n", key,
                                 (int)(postingSize[i]), m_pBlockController.TotalBlocks());
                    if (m_fileIoUseLock)
                    {
                        m_rwMutex[hash(key)].unlock();
                    }
                    return ErrorCode::Block_IDError;
                }
                if (checked[postingSize[i]])
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Check] Block %lld double used!\n", postingSize[i]);
                    return ErrorCode::Block_IDError;
                }
                else
                {
                    checked[postingSize[i]] = true;
                }
            }
            if (m_fileIoUseLock)
            {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        void PrintPostingDiff(std::string& p1, std::string& p2, const char* pos) {
            if (p1.size() != p2.size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Merge %s: p1 and p2 have different sizes: before=%u after=%u\n", pos, p1.size(), p2.size());
                return;
            }
            std::string diff = "";
            for (size_t i = 0; i < p1.size(); i+=4) {
                if (p1[i] != p2[i]) {
                    diff += "[" + std::to_string(i) + "]:" + std::to_string(int(p1[i])) + "^" + std::to_string(int(p2[i])) + " ";
                }
            }
            if (diff.size() != 0) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Merge %s: %s\n", pos, diff.c_str());
            }
        }

        ErrorCode Merge(const SizeType key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Key range error: key: %d, mapping size: %d\n", key, r);
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Key_OverFlow;
            }
            
            if (m_pShardedLRUCache) {
                if (m_pShardedLRUCache->merge(key, (void *)(value.data()), value.size())) {
                    return ErrorCode::Success;
                }
            }

            int64_t* postingSize = (int64_t*)At(key);
            if (((uintptr_t)postingSize) == 0xffffffffffffffff || *postingSize < 0)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Merge] Key %d failed: postingSize < 0\n", key);
                if (m_fileIoUseLock)
                {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Key_NotFound;
                //return Put(key, value, timeout, reqs);
            }

            auto newSize = *postingSize + value.size();
            int newblocks = ((newSize + PageSize - 1) >> PageSizeEx);
            if (newblocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "[Merge] Key %d failed: new size %lld bytes requires %d blocks (limit: %d)\n",
                    key, static_cast<long long>(newSize), newblocks, m_blockLimit);

                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                    "[Merge] Original size: %lld bytes, Merge size: %lld bytes\n",
                    static_cast<long long>(*postingSize), static_cast<long long>(value.size()));

                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Posting_OverFlow;
            }

            //std::string before;
            //Get(key, &before, timeout, reqs);

            auto sizeInPage = (*postingSize) % PageSize;    // Actual size of the last block
            int oldblocks = (*postingSize >> PageSizeEx);
            int allocblocks = newblocks - oldblocks;
            // If the last block is not full, we need to read it first, then append the new data, and write it back.
            if (sizeInPage != 0) {
                std::string newValue;
                AddressType readreq[] = { sizeInPage, *(postingSize + 1 + oldblocks) };
                if (!m_pBlockController.ReadBlocks(readreq, &newValue, timeout, reqs))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Merge] Cannot read original posting!\n");
                    return ErrorCode::DiskIOFail;
                }
                //std::string lastblock = before.substr(before.size() - sizeInPage);
                //PrintPostingDiff(lastblock, newValue, "0");
                newValue += value;

                uintptr_t tmpblocks = 0xffffffffffffffff;
                while (!m_buffer.try_pop(tmpblocks));
                memcpy((AddressType*)tmpblocks, postingSize, sizeof(AddressType) * (oldblocks + 1));
                if (!m_pBlockController.GetBlocks((AddressType *)tmpblocks + 1 + oldblocks, allocblocks))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Merge] Not enough blocks in the pool can be allocated!\n");
                    m_buffer.push(tmpblocks);
                    return ErrorCode::DiskIOFail;
                }
                if (!m_pBlockController.WriteBlocks((AddressType *)tmpblocks + 1 + oldblocks, allocblocks, newValue,
                                                    timeout, reqs))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "[Merge] Write new block failed!\n");
                    m_pBlockController.ReleaseBlocks((AddressType *)tmpblocks + 1 + oldblocks, allocblocks);
                    m_buffer.push(tmpblocks);
                    return ErrorCode::DiskIOFail;
                }
                *((int64_t*)tmpblocks) = newSize;

                // This is also to ensure checkpoint correctness, so we release the partially used block and allocate a new one.
                m_pBlockController.ReleaseBlocks(postingSize + 1 + oldblocks, 1);
                while (InterlockedCompareExchange(&At(key), tmpblocks, (uintptr_t)postingSize) != (uintptr_t)postingSize) {
                    postingSize = (int64_t*)At(key);
                }
                m_buffer.push((uintptr_t)postingSize);
            }
            else {  // Otherwise, directly allocate a new batch of blocks to append after the current ones.
                if (!m_pBlockController.GetBlocks(postingSize + 1 + oldblocks, allocblocks))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "[Merge] Not enough blocks in the pool can be allocated!\n");
                    return ErrorCode::DiskIOFail;
                }
                if (!m_pBlockController.WriteBlocks(postingSize + 1 + oldblocks, allocblocks, value, timeout, reqs))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[Merge] Write new block failed!\n");
                    m_pBlockController.ReleaseBlocks(postingSize + 1 + oldblocks, allocblocks);
                    return ErrorCode::DiskIOFail;
                }
                *postingSize = newSize;
            }
	    /*
            std::string after;
            Get(key, &after, timeout, reqs);
            before += value;
            PrintPostingDiff(before, after, "1");
            */
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Merge(const std::string &key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
            return Merge(std::stoi(key), value, timeout, reqs);
        }

        ErrorCode Delete(SizeType key) override {
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) return ErrorCode::Key_OverFlow;

            if (m_pShardedLRUCache) {
                m_pShardedLRUCache->del(key);
            }

            int64_t* postingSize = (int64_t*)At(key);
            if (((uintptr_t)postingSize) == 0xffffffffffffffff || *postingSize < 0)
            {
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Key_NotFound;
            }

            int blocks = ((*postingSize + PageSize - 1) >> PageSizeEx);
            m_pBlockController.ReleaseBlocks(postingSize + 1, blocks);
            m_buffer.push((uintptr_t)postingSize);
            //m_key_reserve.push(key);
            /*
            while (m_key_reserve.unsafe_size() > m_bufferLimit)
            {
                SizeType cleanKey = 0;
                if (m_key_reserve.try_pop(cleanKey))
                {
                    At(cleanKey) = 0xffffffffffffffff;
                }
            }
            */
            At(key) = 0xffffffffffffffff;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Delete(const std::string &key) {
            return Delete(std::stoi(key));
        }

        void ForceCompaction() {
            Save(m_mappingPath);
        }

        void GetStat() {
            int remainBlocks = m_pBlockController.RemainBlocks();
            int reserveBlocks = m_pBlockController.ReserveBlocks();
            int totalBlocks = m_pBlockController.TotalBlocks();
            int remainGB = ((long long)(remainBlocks + reserveBlocks) >> (30 - PageSizeEx));
            // int remainGB = remainBlocks >> 20 << 2;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Total %d blocks, Remain %d blocks, Reserve %d blocks, totally %d GB\n", totalBlocks, remainBlocks, reserveBlocks, remainGB);
            double average_read_time = (double)read_time_vec / get_times_vec;
            double average_get_time = (double)get_time_vec / get_times_vec;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Get times: %llu, get time: %llu us, read time: %llu us\n", get_times_vec.load(), get_time_vec.load(), read_time_vec.load());
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Average read time: %lf us, average get time: %lf us\n", average_read_time, average_get_time);
            if (m_pShardedLRUCache) {
                auto cache_stat = m_pShardedLRUCache->get_stat();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Cache queries: %lld, Cache hits: %lld, Hit rates: %lf\n", cache_stat.first, cache_stat.second, cache_stat.second == 0 ? 0 : (double)cache_stat.second / cache_stat.first);
            }
            m_pBlockController.IOStatistics();
        }

        ErrorCode Load(std::string path, SizeType blockSize, SizeType capacity) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load mapping From %s\n", path.c_str());
            auto ptr = f_createIO();
            if (ptr == nullptr || !ptr->Initialize(path.c_str(), std::ios::binary | std::ios::in)) return ErrorCode::FailedOpenFile;

            SizeType CR, mycols;
            IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&CR);
            IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&mycols);
            if (mycols > m_blockLimit) m_blockLimit = mycols;

            m_pBlockMapping.Initialize(CR, 1, blockSize, capacity);
            for (int i = 0; i < CR; i++) {
                At(i) = (uintptr_t)(new AddressType[m_blockLimit]);
                IOBINARY(ptr, ReadBinary, sizeof(AddressType) * mycols, (char*)At(i));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load mapping (%d,%d) Finish!\n", CR, mycols);
            return ErrorCode::Success;
        }
        
        ErrorCode Save(std::string path) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save mapping To %s\n", path.c_str());
            if (m_pShardedLRUCache) {
                if (!m_pShardedLRUCache->flush()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to flush cache when saving mapping!\n");
                    return ErrorCode::Fail;
                }
            }

            auto ptr = f_createIO();
            if (ptr == nullptr || !ptr->Initialize(path.c_str(), std::ios::binary | std::ios::out)) return ErrorCode::FailedCreateFile;

            SizeType CR = m_pBlockMapping.R();
            IOBINARY(ptr, WriteBinary, sizeof(SizeType), (char*)&CR);
            IOBINARY(ptr, WriteBinary, sizeof(SizeType), (char*)&m_blockLimit);
            std::vector<AddressType> empty(m_blockLimit, 0xffffffffffffffff);
            for (int i = 0; i < CR; i++) {
                if (At(i) == 0xffffffffffffffff) {
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType) * m_blockLimit, (char*)(empty.data()));
                }
                else {
                    int64_t* postingSize = (int64_t*)At(i);
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType) * m_blockLimit, (char*)postingSize);
                }
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save mapping (%d,%d) Finish!\n", CR, m_blockLimit);
	    /*
            for (int i = 0; i < 10; i++) {
                std::cout << "i=" << i << ": ";
                for (int j = 0; j < 10; j++) {
                    std::cout << m_pBlockMapping[i][j] << " ";
                }
                std::cout << std::endl;
            }
            for (int i = m_pBlockMapping.R() - 10; i < m_pBlockMapping.R(); i++) {
                std::cout << "i=" << i << ": ";
                for (int j = 0; j < 10; j++) {
                    std::cout << m_pBlockMapping[i][j] << " ";
                }
                std::cout << std::endl;
            }
	    */
            return ErrorCode::Success;
        }

        ErrorCode Checkpoint(std::string prefix) override {
            std::string filename = prefix + FolderSep + m_mappingPath.substr(m_mappingPath.find_last_of(FolderSep) + 1);
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: saving block mapping to %s\n", filename.c_str());
            Save(filename);
            return m_pBlockController.Checkpoint(filename + "_postings");
        }

    private:
        static constexpr const char* kFileIoUseLock = "SPFRESH_FILE_IO_USE_LOCK";
        static constexpr bool kFileIoDefaultUseLock = false;
        static constexpr const char* kFileIoLockSize = "SPFRESH_FILE_IO_LOCK_SIZE";
        static constexpr int kFileIoDefaultLockSize = 1024;

	    std::atomic<uint64_t> read_time_vec = 0;
	    std::atomic<uint64_t> get_time_vec = 0;
	    std::atomic<uint64_t> get_times_vec = 0;

        bool m_fileIoUseLock = kFileIoDefaultUseLock;
        int m_fileIoLockSize = kFileIoDefaultLockSize;
        std::string m_mappingPath;
        SizeType m_blockLimit;
        COMMON::Dataset<uintptr_t> m_pBlockMapping;
        SizeType m_bufferLimit;
        Helper::Concurrent::ConcurrentQueue<uintptr_t> m_buffer;
        Helper::Concurrent::ConcurrentQueue<SizeType> m_key_reserve;

        std::shared_ptr<Helper::ThreadPool> m_compactionThreadPool;
        BlockController m_pBlockController;
        ShardedLRUCache *m_pShardedLRUCache;

        bool m_shutdownCalled;
        std::shared_mutex m_updateMutex;
        std::vector<std::shared_mutex> m_rwMutex;

        inline int hash(int key) {
            return key % m_fileIoLockSize;
        }
    };
}
#endif
