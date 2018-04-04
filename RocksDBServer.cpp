/**
 * This is a implementation of the mapkeeper interface that uses 
 * rocksdb.
 *
 */
#include <cstdio>
#include "MapKeeper.h"
#include <rocksdb/db_bench_tool.h>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <arpa/inet.h>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <string>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace boost::filesystem;

using namespace mapkeeper;

using namespace rocksdb;
using std::string;

int syncmode;
int blindinsert;
int blindupdate;
string path;
class RocksDBServer: virtual public MapKeeperIf {
public:
    RocksDBServer(const std::string& directoryName) : 
        directoryName_(directoryName) {

        // open all the existing databases
        // leveldb::DB* db;
        // leveldb::Options options;
        // options.create_if_missing = false;
        // options.error_if_exists = false;
        // options.write_buffer_size = 500 * 1048576; // 500MB write buffer
        // options.block_cache = leveldb::NewLRUCache(10000L * 1048576L);  // 1.5GB cache
        // options.compression = leveldb::kNoCompression;

          DB* db;
          Options options;
          // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
          options.IncreaseParallelism();
          options.OptimizeLevelStyleCompaction();
          // create the DB if it's not already present
          options.create_if_missing = true;

        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;

        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr;itr++) {
            if (is_directory(itr->status())) {
                std::string mapName = itr->path().filename().string();
                Status status = rocksdb::DB::Open(options, itr->path().string(), &db);
                assert(status.ok());
                maps_.insert(mapName, db);
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        // leveldb::DB* db;
        // leveldb::Options options;
        // options.create_if_missing = true;
        // options.error_if_exists = true;
        // options.write_buffer_size = 500 * 1048576; // 500MB write buffer
        // options.block_cache = leveldb::NewLRUCache(1500 * 1048576);  // 1.5GB cache
          DB* db;
          Options options;
          // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
          options.IncreaseParallelism();
          options.OptimizeLevelStyleCompaction();
          // create the DB if it's not already present
          options.create_if_missing = true;

        Status status = rocksdb::DB::Open(options, directoryName_ + "/" + mapName, &db);
        if (!status.ok()) {
            // TODO check return code
            printf("status: %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        std::string mapName_ = mapName;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        DIR *dp;
        struct dirent *dirp;
        if((dp  = opendir(directoryName_.c_str())) == NULL) {
            _return.responseCode = ResponseCode::Success;
            return;
        }

        while ((dirp = readdir(dp)) != NULL) {
            _return.values.push_back(std::string(dirp->d_name));
        }
        closedir(dp);
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        if (order == mapkeeper::ScanOrder::Ascending){
            scanAscending(_return, mapName, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        } else if (order == mapkeeper::ScanOrder::Descending) {
            scanDescending(_return, mapName, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        } else {
            _return.responseCode = mapkeeper::ResponseCode::Error;
        }
    }

    

    void scanAscending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }

        ReadOptions options(true, true);
        options.tailing = false;
        Iterator* iter = itr->second->NewIterator(options);
        Comparator* cmp = itr->second->DefaultColumnFamily()->GetComparator();
        iter->Seek(startKey);
        if (!startKeyIncluded && cmp->Compare(startKey, iter->value()) == 0)
            iter->Next();
        for (; iter->valid() && (endKeyIncluded) ? (cmp->Compare(iter->value(), endKey)<=0):(cmp->Compare(iter->value(), endKey)<0); iter->Next()) {
            mapkeeper::Record record;
            record.key = iter->key();
            record.value = iter->value();
            numBytes += iter->key().size() + iter->value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = mapkeeper::ResponseCode::Success;
                return;
            }
        }
        delete iter;

        _return.responseCode = ResponseCode::ScanEnded;
    }

    void scanDescending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }

        ReadOptions options(true, true);
        options.tailing = false;
        Iterator* iter = itr->second->NewIterator(options);
        Comparator* cmp = itr->second->DefaultColumnFamily()->GetComparator();
        iter->Seek(startKey);
        if (!startKeyIncluded && cmp->Compare(startKey, iter->value()) == 0)
            iter->Prev();
        for (; iter->valid() && (endKeyIncluded) ? (cmp->Compare(iter->value(), endKey)>=0):(cmp->Compare(iter->value(), endKey)>0); iter->Prev()) {
            mapkeeper::Record record;
            record.key = iter->key();
            record.value = iter->value();
            numBytes += iter->key().size() + iter->value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = mapkeeper::ResponseCode::Success;
                return;
            }
        }
        delete iter;
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &(_return.value));
        if (status.IsNotFound()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (!status.ok()) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;

        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
        rocksdb::Status status = itr->second->Put(options, key, value);

        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
	if(!blindinsert) {
	  std::string recordValue;
	  rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &recordValue);
	  if (status.ok()) {
            printf("Record exists!\n");
            return ResponseCode::RecordExists;
	  } else if (!status.IsNotFound()) {
            return ResponseCode::Error;
	  }
	}
        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
	rocksdb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            printf("insert not ok! %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        std::string recordValue;
	if(!blindupdate) {
	  rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &recordValue);
	  if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
	  } else if (!status.ok()) {
            return ResponseCode::Error;
	  }
	}
        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
	rocksdb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        rocksdb::WriteOptions options;
        options.sync = false;
        rocksdb::Status status = itr->second->Delete(options, key);
        if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
        } else if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    boost::ptr_map<std::string, rocksdb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    if(argc != 5) { printf("Usage: %s <sync:0 or 1> <blindinsert:0 or 1> <blindupdate:0 or 1> <path> \n", argv[0]); }
    syncmode    = atoi(argv[1]);
    blindinsert = atoi(argv[2]);
    blindupdate = atoi(argv[3]);
    path = string(argv[4]);
    int port = 9090;
    shared_ptr<RocksDBServer> handler(new RocksDBServer(path));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
