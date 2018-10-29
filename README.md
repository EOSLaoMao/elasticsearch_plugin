# elasticsearch_plugin

Nodeos plugin for archiving blockchain data into Elasticsearch, inspired by [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin).

**Currently the plugin only work with [official eosio repository](https://github.com/EOSIO/eos).**

## Benchmark

Detail: [Benchmark](./benchmark/benchmark.md)

### Replay 10000 Block

|                      | elapse(s) | speed(b/s) |
| -------------------- |:---------:|:----------:|
| elasticsearch_plugin | 266       | 37.59      |
| mongo_db_plugin      | 694       | 14.41      |

### Replay 100000 Block

|                      | elapse(s) | speed(b/s) |
| -------------------- |:---------:|:----------:|
| elasticsearch_plugin | 354       | 282.49     |
| mongo_db_plugin      | 987       | 101.32     |

## Performance Tuning

Example filters:

```text
--elastic-filter-out=eosio:onblock:
--elastic-filter-out=gu2tembqgage::
--elastic-filter-out=blocktwitter::
```

[Tune for indexing speed](https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-indexing-speed.html)

In the benchmark, `elasticsearch_plugin` is running with default config. For production deploy, you can tweak some config.

```text
  --elastic-abi-cache-size arg (=2048)           The maximum size of the abi cache for serializing data.
  --elastic-thread-pool-size arg (=4)            The size of the data processing thread pool.
  --elastic-bulker-pool-size arg (=2)            The size of the elasticsearch bulker pool.
  --elastic-bulk-size arg (=5)                   The size(megabytes) of the each bulk request.
```

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
cd plugins/elasticsearch_plugin
git submodule update --init --recursive
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
  -q [ --elastic-queue-size ] arg (=1024)       The target queue size between nodeos 
                                                and elasticsearch plugin thread.
  --elastic-abi-cache-size arg (=2048)          The maximum size of the abi cache for 
                                                serializing data.
  --elastic-thread-pool-size arg (=4)           The size of the data processing thread 
                                                pool.
  --elastic-bulker-pool-size arg (=2)           The size of the elasticsearch bulker 
                                                pool.
  --elastic-bulk-size arg (=5)                  The size(megabytes) of the each bulk 
                                                request.
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
  --elastic-filter-on arg                       Track actions which match 
                                                receiver:action:actor. Receiver, 
                                                Action, & Actor may be blank to include
                                                all. i.e. eosio:: or :transfer:  Use * 
                                                or leave unspecified to include all.
  --elastic-filter-out arg                      Do not track actions which match 
                                                receiver:action:actor. Receiver, 
                                                Action, & Actor may be blank to exclude
                                                all.
```

## TODO

- [ ] Due to `libcurl` [100-continue feature](https://curl.haxx.se/mail/lib-2017-07/0013.html), consider replace [EOSLaoMao/elasticlient](https://github.com/EOSLaoMao/elasticlient) with other simple http client like [https://cpp-netlib.org/#](https://cpp-netlib.org/#)
