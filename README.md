# elasticsearch_plugin

Nodeos plugin for archiving blockchain data into Elasticsearch, inspired by [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin).

**Currently the plugin only work with [official eosio repository](https://github.com/EOSIO/eos).**

## Getting Help
- For questions about this project:
  - [Join our Telegram group](https://t.me/eosesplugin)
  - [Open an issue](https://github.com/EOSLaoMao/elasticsearch_plugin/issues/new)

## Indices

It is recommended to use other tools for indices management. Checkout [EOSLaoMao/elasticsearch-node](https://github.com/EOSLaoMao/elasticsearch-node).  

[Document examples](#document-examples)

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
  --elastic-thread-pool-size arg (=4)            The size of the data processing thread pool.
  --elastic-bulk-size-mb arg (=5)                The size(megabytes) of the each bulk request.
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

The usage of `elasticsearch_plugin` is similar to [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin). It is recommended that a large `--abi-serializer-max-time-ms` value be passed into the nodeos running the elasticsearch_plugin as the default abi serializer time limit is not large enough to serialize large blocks.

```plain
Config Options for eosio::elasticsearch_plugin.
  -q [ --elastic-queue-size ] arg (=1024)       The target queue size between nodeos 
                                                and elasticsearch plugin thread.
  --elastic-thread-pool-size arg (=4)           The size of the data processing thread 
                                                pool.
  --elastic-bulk-size-mb arg (=5)               The size(megabytes) of the each bulk 
                                                request.
  --elastic-abi-db-size-mb arg (=1024)          Maximum size(megabytes) of the abi 
                                                database.
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


## Document examples

* accounts
```
{
  "creator": "eosio",
  "pub_keys": [
    {
      "permission": "owner",
      "key": "EOS5Ga8VeykSY7SXJyHbnanSPHPcQ3LmKDtJjJBJHokgYDxeokP4R"
    },
    {
      "permission": "active",
      "key": "EOS5Ga8VeykSY7SXJyHbnanSPHPcQ3LmKDtJjJBJHokgYDxeokP4R"
    }
  ],
  "account_create_time": "2018-06-09T12:01:56.500",
  "account_controls": [],
  "name": "heztcnjtguge"
}
```

* action_traces
```
{
  "receipt": {
    "receiver": "eosio.token",
    "act_digest": "94753d3277e8759aaa7e4ee8f19cfa36013180e0d66d62a4a2196d76786d574a",
    "global_sequence": 1730634,
    "recv_sequence": 459727,
    "auth_sequence": [
      [
        "eosio",
        1730627
      ]
    ],
    "code_sequence": 1,
    "abi_sequence": 1
  },
  "act": {
    "account": "eosio.token",
    "name": "transfer",
    "authorization": [
      {
        "actor": "eosio",
        "permission": "active"
      }
    ],
    "data": "{\"from\":\"eosio\",\"to\":\"eosio.ram\",\"quantity\":\"0.1219 EOS\",\"memo\":\"buy ram\"}",
    "hex_data": "0000000000ea3055000090e602ea3055c30400000000000004454f5300000000076275792072616d"
  },
  "context_free": false,
  "elapsed": 80,
  "console": "",
  "trx_id": "98a9839544b7f4ae6ebc5fa1f9f17bd31027c4d845b7af9a6fa8cc8f9fafb0f1",
  "block_num": 6643,
  "block_time": "2018-06-09T12:53:42.500",
  "producer_block_id": "000019f31e63bace5345f703d5140daeada8453abec7d5ebeb2d735b859e8e07",
  "account_ram_deltas": [],
  "except": null
}
```

* transaction_traces
```
{
  "id": "711c6dab0e1b1a90aff0c54c3aed64f870a77ad35fd0aa6a19c6542c55653fd3",
  "block_num": 7167,
  "block_time": "2018-06-09T12:58:06.000",
  "producer_block_id": "00001bff0cc43235d99514029440013e2fb7b6bd5eb6fb5c848954a088a9e0f9",
  "receipt": {
    "status": "executed",
    "cpu_usage_us": 129052,
    "net_usage_words": 1298
  },
  "elapsed": 44456,
  "net_usage": 10384,
  "scheduled": false,
  "action_traces": [
    {
      "receipt": {
        "receiver": "eosio",
        "act_digest": "6556d890f6238c244020411322d6a40b5c03519e3da17dc05f525387ae850914",
        "global_sequence": 1874256,
        "recv_sequence": 878486,
        "auth_sequence": [
          [
            "eosio",
            1874249
          ]
        ],
        "code_sequence": 2,
        "abi_sequence": 2
      },
      "act": {
        "account": "eosio",
        "name": "newaccount",
        "authorization": [
          {
            "actor": "eosio",
            "permission": "active"
          }
        ],
        "data": {
          "creator": "eosio",
          "name": "gi4tcnzwhege",
          "owner": {
            "threshold": 1,
            "keys": [
              {
                "key": "EOS8M7Qbuq2U5PBph9QhBm5o7sHzuxCgMewwWnmLk9be2WshZEXZB",
                "weight": 1
              }
            ],
            "accounts": [],
            "waits": []
          },
          "active": {
            "threshold": 1,
            "keys": [
              {
                "key": "EOS8M7Qbuq2U5PBph9QhBm5o7sHzuxCgMewwWnmLk9be2WshZEXZB",
                "weight": 1
              }
            ],
            "accounts": [],
            "waits": []
          }
        },
        "hex_data": "0000000000ea3055a0986afc4f94896301000000010003c7893323d9b3bd2ad4aab6708177e17a7254fdd28cc5700a54fbe3d5869e424d0100000001000000010003c7893323d9b3bd2ad4aab6708177e17a7254fdd28cc5700a54fbe3d5869e424d01000000"
      },
      "context_free": false,
      "elapsed": 184,
      "console": "",
      "trx_id": "711c6dab0e1b1a90aff0c54c3aed64f870a77ad35fd0aa6a19c6542c55653fd3",
      "block_num": 7167,
      "block_time": "2018-06-09T12:58:06.000",
      "producer_block_id": "00001bff0cc43235d99514029440013e2fb7b6bd5eb6fb5c848954a088a9e0f9",
      "account_ram_deltas": [
        {
          "account": "gi4tcnzwhege",
          "delta": 2996
        }
      ],
      "except": null,
      "inline_traces": []
    }
  ],
  "except": null
}
```

* blocks
```
{
  "timestamp": "2018-06-09T12:03:37.500",
  "producer": "eosio",
  "confirmed": 0,
  "previous": "00000288461e6ef331cb87ebf3e8fda122dc4c1b5cfd313b080a762ce0a472df",
  "transaction_mroot": "a850a72dc0fc67adce701455551c1d82246fae12055f5d10b66ba55921a0dc34",
  "action_mroot": "1b79fd3dd0e544c4eacce998cf6d9a7fa9a3d4533590779b94d2be002fd52618",
  "schedule_version": 0,
  "new_producers": null,
  "header_extensions": [],
  "producer_signature": "SIG_K1_KfX4VW3aLd2KjsJCqWQpDdauT7xJwERXpckJhUG8UPNANtgqVjRZEJBGKH7DJ7ZJr9E1mmTkzqSz4KQ2FyLBpAv4KPqpxt",
  "transactions": [...],
  "block_extensions": [],
  "irreversible": true,
  "validated": true
}
```

* block_states
```
{
  "active_schedule": {
    "version": 1,
    "producers": [
      {
        "producer_name": "genesisblock",
        "block_signing_key": "EOS8Yid3mE5bwWMvGGKYEDxFRGHostu5xCzFanyJP1UdgZ5mpPdwZ"
      }
    ]
  },
  "in_current_chain": true,
  "pending_schedule_lib_num": 12149,
  "dpos_proposed_irreversible_blocknum": 154026,
  "dpos_irreversible_blocknum": 154025,
  "pending_schedule": {
    "version": 1,
    "producers": []
  },
  "producer_to_last_produced": [
    [
      "eosio",
      12150
    ],
    [
      "genesisblock",
      154026
    ]
  ],
  "pending_schedule_hash": "c43882d5411af19d8596d5d835b3f4bd6a7fd36cc4c7fb55942ef11f8d1473b6",
  "blockroot_merkle": {
    "_active_nodes": [
      "000259a9acccbad8e577c2ec8bd87e2e956b21ebdf1524c01ea65dd464ce0fec",
      "3323314d8d9f2eed9233911c01ec52a0b22ec59c5b2902ac9396080bad602a90",
      "df39e691954565362fbedd6be43667b551594a7e37ad1f7e2d4f8f25c545b1e8",
      "6931158aae9e5f5391ca559eb0503f1631de3ddff73785d3bf98a12418cbfe77",
      "e4e2fdd9850efba91ed0f12efcecebe54b37ce9264b50722948dd23a8fb49f62",
      "2930747c325841aa6926c3b8143402bb5e35c88f1a5af76cf2c9c09761be9863",
      "2cde046c52b281ed55abb64a9b3da2882c19bd8100734bf9774f94b539870479",
      "93eade5dac8991d23381f380f9121568c47b9e3c37317a5de3c28503aae93e53",
      "827270b90af501d41051054f541ec063dfbab4e4a2eb4d6f85ffac575df777f0",
      "0f8be62807a52c66552ac3ffb9406fe9b644d4c7a38fc195205c6a16c549aa3b"
    ],
    "_node_count": 154025
  },
  "confirm_count": [],
  "confirmations": [],
  "block_num": 154026,
  "validated": true,
  "bft_irreversible_blocknum": 0,
  "header": {
    "header_extensions": [],
    "previous": "000259a9acccbad8e577c2ec8bd87e2e956b21ebdf1524c01ea65dd464ce0fec",
    "schedule_version": 1,
    "producer": "genesisblock",
    "transaction_mroot": "0000000000000000000000000000000000000000000000000000000000000000",
    "producer_signature": "SIG_K1_K7EFKTaKJrEeQLXSdmBoC15yva6otnavPq4LGNZcojuSYtsnLuPtZDBf7Sqgw3U2cGWjnS7Ncg5cY2KLNiUFMKYgAP9D3m",
    "confirmed": 0,
    "action_mroot": "18343eeab718c318d906b20cee90c8c6eeeffef04cdd514f0a22e75f24c21cf1",
    "timestamp": "2018-06-11T08:21:42.000"
  },
  "producer_to_last_implied_irb": [
    [
      "genesisblock",
      154025
    ]
  ],
  "block_signing_key": "EOS8Yid3mE5bwWMvGGKYEDxFRGHostu5xCzFanyJP1UdgZ5mpPdwZ",
  "id": "000259aaeecdcdd5af5196321bc74643169962a4c20dff0cacb6225c4a7ab2c2",
  "irreversible": true
}
```
