# elasticsearch_plugin
Nodeos plugin for archiving blockchain data into Elasticsearch, inspired by [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin).

**Currently the plugin only work with [official eosio repository](https://github.com/EOSIO/eos).**

[Benchmark](benchmark.md)

## Installation

### Install `EOSLaoMao/elasticlient`

`elasticsearh_plugin` rely on [EOSLaoMao/elasticlient](https://github.com/EOSLaoMao/elasticlient)

```bash
git clone https://github.com/WLBF/elasticlient.git
cd elasticlient
git submodule update --init --recursive
cmake -DBUILD_ELASTICLIENT_TESTS=NO -DBUILD_ELASTICLIENT_EXAMPLE=NO
make
sudo make install
# copy cpr library manually
sudo cp -r "external/cpr/include/cpr" "/usr/local/include/cpr"
sudo cp "lib/libcpr.so" "/usr/local/lib/libcpr.so"
```

### Embed `elasticsearch_plugin` into `nodeos`

1. Get `elasticsearch_plugin` source code.

```bash
git clone https://github.com/EOSLaoMao/elasticsearch_plugin.git plugins/elasticsearch_plugin
```

2. Add subdirectory to `plugins/CMakeLists.txt`.

```cmake
...
add_subdirectory(mongo_db_plugin)
add_subdirectory(login_plugin)
add_subdirectory(login_plugin)
add_subdirectory(elasticsearch_plugin) # add this line.
...
```

3. Add following line to `programs/nodeos/CMakeLists.txt`.

```cmake
target_link_libraries( ${NODE_EXECUTABLE_NAME}
        PRIVATE appbase
        PRIVATE -Wl,${whole_archive_flag} login_plugin               -Wl,${no_whole_archive_flag}
        PRIVATE -Wl,${whole_archive_flag} history_plugin             -Wl,${no_whole_archive_flag}
        ...
        # add this line.
        PRIVATE -Wl,${whole_archive_flag} elasticsearch_plugin       -Wl,${no_whole_archive_flag}
        ...
```

## Usage

The usage of `elasticsearch_plugin` is similar to [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin).

```plain
Config Options for eosio::elasticsearch_plugin:
  -q [ --elastic-queue-size ] arg (=512)        The target queue size between nodeos 
                                                and elasticsearch plugin thread.
  --elastic-abi-cache-size arg (=2048)          The maximum size of the abi cache for 
                                                serializing data.
  --elastic-index-wipe                          Required with --replay-blockchain, 
                                                --hard-replay-blockchain, or 
                                                --delete-all-blocks to delete 
                                                elasticsearch index.This option 
                                                required to prevent accidental wipe of 
                                                index.
  --elastic-block-start arg (=0)                If specified then only abi data pushed 
                                                to elasticsearch until specified block 
                                                is reached.
  -u [ --elastic-url ] arg                      elasticsearch URL connection string If 
                                                not specified then plugin is disabled.
  --elastic-user arg                            elasticsearch user.
  --elastic-password arg                        elasticsearch password.
  --elastic-store-blocks arg (=1)               Enables storing blocks in 
                                                elasticsearch.
  --elastic-store-block-states arg (=1)         Enables storing block state in 
                                                elasticsearch.
  --elastic-store-transactions arg (=1)         Enables storing transactions in 
                                                elasticsearch.
  --elastic-store-transaction-traces arg (=1)   Enables storing transaction traces in 
                                                elasticsearch.
  --elastic-store-action-traces arg (=1)        Enables storing action traces in 
                                                elasticsearch.
  --elasticsearch-filter-on arg                 elasticsearch: Track actions which 
                                                match receiver:action:actor. Actor may 
                                                be blank to include all. Receiver and 
                                                Action may not be blank. Default is * 
                                                include everything.
  --elasticsearch-filter-out arg                elasticsearch: Do not track actions 
                                                which match receiver:action:actor. 
                                                Action and Actor both blank excludes 
                                                all from reciever. Actor blank excludes
                                                all from reciever:action. Receiver may 
                                                not be blank.
```
