/**
 * This is a implementation of the mapkeeper interface that uses 
 * leveldb.
 *
 * http://leveldb.googlecode.com/svn/trunk/doc/index.html
 */
#include <cstdio>
#include "MapKeeper.h"
#include "pmemkv.h"
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using namespace pmemkv;

using boost::shared_ptr;
using namespace boost::filesystem;

using namespace mapkeeper;
using std::cout;
using std::endl;
using std::cin;

int syncmode;
int blindinsert;
int blindupdate;



class PmemkvServer: virtual public MapKeeperIf {
public:
    PmemkvServer(const std::string& directoryName) : 
        directoryName_(directoryName) {

        // open all the existing databases
        //leveldb::DB* db;
        //leveldb::Options options;
        //options.create_if_missing = false;
        //options.error_if_exists = false;
        //options.write_buffer_size = 500 * 1048576; // 500MB write buffer
        //options.block_cache = leveldb::NewLRUCache(10000L * 1048576L);  // 1.5GB cache
        //options.compression = leveldb::kNoCompression;

        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;

        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr;itr++) {
            if (is_directory(itr->status())) {
                std::string mapName = itr->path().filename().string();
                KVEngine* kv = KVEngine::Open("kvtree", itr->path().string(), PMEMOBJ_MIN_POOL);
                //cout << "itr->path().filename() " << mapName << endl;
                /cout << "itr->path().string() " << itr->path().string() << endl;
                //leveldb::Status status = leveldb::DB::Open(options, itr->path().string(), &db);
                //assert(status.ok());
                maps_.insert(mapName, kv);
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& filename) {
        //leveldb::DB* db;
        //leveldb::Options options;
        //options.create_if_missing = true;
        //options.error_if_exists = true;
        //options.write_buffer_size = 500 * 1048576; // 500MB write buffer
        //options.block_cache = leveldb::NewLRUCache(1500 * 1048576);  // 1.5GB cache
        //leveldb::Status status = leveldb::DB::Open(options, directoryName_ + "/" + mapName, &db);
        //if (!status.ok()) {
            // TODO check return code
        //    printf("status: %s\n", status.ToString().c_str());
        //    return ResponseCode::Error;
        //}
        KVEngine* kv = KVEngine::Open("kvtree", directoryName_ + "/" + filename, PMEMOBJ_MIN_POOL);
        std::string mapName_ = pathname;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        maps_.insert(mapName_, kv);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& filename) {
        std::string mapName_ = filename;
        boost::ptr_map<std::string, pmemkv::KVEngine*>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        //delete (itr->second);
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
    }

    void scanAscending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void scanDescending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        //leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &(_return.value));
        

        pmemkv::KVStatus s = itr->second->Get(key, &(_return.value));

        if (s == NOT_FOUND) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (s == FAILED) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, pmemkv::KVEngine>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;

        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        //leveldb::WriteOptions options;
        //options.sync = syncmode ? true : false;
        //leveldb::Status status = itr->second->Put(options, key, value);
        pmemkv::KVStatus s = itr->second->Put(key, value);

        if (s == FAILED) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
	if(!blindinsert) {
	  std::string recordValue;
	  //leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
      pmemkv::KVStatus s = itr->second->Get(key, recordValue);

	  if (s == OK) {
            printf("Record exists!\n");
            return ResponseCode::RecordExists;
	  } else if (s == FAILED) {
            return ResponseCode::Error;
	  }
	}
        //leveldb::WriteOptions options;
        //options.sync = syncmode ? true : false;
	//leveldb::Status status = itr->second->Put(options, key, value);
        pmemkv::KVStatus s = itr->second->Put(key, value);

        if (s == FAILED) {
            printf("insert not ok! %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, pmemkv::KVEngine>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        std::string recordValue;
	if(!blindupdate) {
	  //leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
	  pmemkv::KVStatus s = itr->second->Get(key, recordValue);

      if (s == NOT_FOUND) {
            return ResponseCode::RecordNotFound;
	  } else if (s == FAILED) {
            return ResponseCode::Error;
	  }
	}
        //leveldb::WriteOptions options;
        //options.sync = syncmode ? true : false;
	leveldb::Status status = itr->second->Put(options, key, value);
        if (s == FAILED) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        //leveldb::WriteOptions options;
        //options.sync = false;
        KVStatus s = itr->second->Remove(key);

        //leveldb::Status status = itr->second->Delete(options, key);
        if (s == NOT_FOUND) {
            return ResponseCode::RecordNotFound;
        } else if (s == FAILED) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    boost::ptr_map<std::string, pmemkv::KVEngine> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    if(argc != 4) { printf("Usage: %s <sync:0 or 1>(unused for pmemkv) <blindinsert:0 or 1> <blindupdate:0 or 1>\n", argv[0]); }
    syncmode    = atoi(argv[1]);
    blindinsert = atoi(argv[2]);
    blindupdate = atoi(argv[3]);
    int port = 9090;
    shared_ptr<PmemkvServer> handler(new PmemkvServer("/dev/shm/pmemkvdir"));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
