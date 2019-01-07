# Set up an elasticsearch cluster stores eos blockchain data

Several weeks ago, our team set up a elasticsearch cluster and replayed a [elasticsearch_plugin](https://github.com/EOSLaoMao/elasticsearch_plugin) added node from scratch. It took us about one week to catch up the head block of the chain. At this moment, the head block number is 36177553, the data size of cluster is around 2.0 terabytes. This is a blog that explains how we managed to do that.

## Set up elasticsearch cluster

When replaying a eos node with elasticsearch plugin, the bottleneck is elasticsearch indexing speed. So our goal is maximizing the indexing performance of elasticsearch cluster. Using faster hardwares like bigger ram and ssd help our cluster perform better. In addition, a proper configuration helps us spend less time and disk space.

### Elasticsearch configuration

Elasticsearch plugin produces several kinds of documents that correspond to data structures predefined in nodeos. We just make some modifications in last commit to remove some redundancy. You can checkout the example here: [Document examples](https://github.com/EOSLaoMao/elasticsearch_plugin/tree/feature/block-structure-change#document-examples).

Index template is great tool for mapping and setting management. In our default template, we set `index.refresh_interval` to -1 and set `index.number_of_replicas` to 0 for better index performance. we also set index sorting and best compression for disk space saving. It is noticed that the type of `act.data` type is set to `text`, this is because we want to make vary structure action data field searchable.

The default template of action_trace index:
```json
{
  "index_patterns" : ["action_traces*"],
  "settings": {
    "index": {
      "number_of_shards": 3,
      "refresh_interval": -1,
      "number_of_replicas": 0,
      "sort.field" : "block_num",
      "sort.order" : "desc"
    },
    "index.codec": "best_compression"
  },
  "mappings": {
    "_doc": {
      "properties": {
        "trx_id": {
          "type": "keyword"
        },
        "producer_block_id": {
          "type": "keyword"
        },
        ...
        "act.data": {
          "type": "text"
        },
        ...
      }
    }
  }
}
```
More detail about templates: [elasticsearch-node/templates](https://github.com/EOSLaoMao/elasticsearch-node/tree/master/templates)

It is important to set up index rollover. We set a cronjob that running curator rollover actions every 10 minutes. This avoid the indexing performance decrease caused by very large shards. By adjusting `conditions.max_size` in curator rollover action file and `number_of_shards` in index template, all shards are maintained in reasonable size.

The curator action traces index rollover action:
```yml
actions:
  ...
  2:
    action: rollover
    description: Rollover the index action_traces
    options:
      disable_action: False
      continue_if_exception: True
      name: action_traces
      conditions:
        max_age: 365d
        max_docs: 300000000
        max_size: 100gb
  ...
```
More detail about curator actions: [elasticsearch-node/curator](https://github.com/EOSLaoMao/elasticsearch-node/tree/master/curator)

### Elasticsearch plugin configuration

It is recommended that the plugin be added to a node that only for data archive purpose. It is also recommended that read-only mode is used to avoid speculative execution. Forked data is still recorded (data that never becomes irreversible) but speculative transaction processing and signaling is avoided minimizing the transaction_traces/action_traces stored.

The elasticsearch plugin is similar to mongodb plugin, action data is stored on chain as raw bytes. This plugin attempts to use associated ABI on accounts to deserialize the raw bytes into explanded abi_def form for storage into elasticsearch. Inside the plugin, ABI information is store in [chainbase](https://github.com/EOSIO/chainbase). The maximum chainbase size is set by option `--elastic-abi-db-size-mb`. Note that invalid or missing abi on a contract will result in the action data being stored as raw bytes. For example the eosio system contract does not provide abi for the onblock action so it is stored as raw bytes.

A larger `--abi-serializer-max-time-ms` value must be passed into the nodeos running the elasticsearch_plugin as the default abi serializer time limit is not large enough to serialize large blocks.

The plugin send bulk requests from multiple threads. Adjusting thread pool size and bulk request size to make better use of the resources of the cluster.

There several switches to control what kinds of documents the plugin send. In our test run, we choose not to store block data by turn option `elastic-store-block` off.

Filter out spam account actions to lower indexing workload or just filter in the specific actions if only interested in certain actions come from certain accounts. Note that transactions that contain only spam actions will also be filtered out.


Our nodeos configuration:
```ini
...
# set max serialize time to 1 second
abi-serializer-max-time-ms = 1000000

read-mode = read-only

elastic-url = http://localhost:9200/
elastic-thread-pool-size = 16
elastic-bulk-size-mb = 20

# do not store block data
elastic-store-block = 0

elastic-filter-out = eosio:onblock:
elastic-filter-out = gu2tembqgage::
elastic-filter-out = blocktwitter::
elastic-filter-out = 1hello1world::
elastic-filter-out = betdicealert::
elastic-filter-out = myeosgateway::
elastic-filter-out = eosonthefly1::
elastic-filter-out = cryptohongbo::
elastic-filter-out = experimentms::
elastic-filter-out = eosplayaloud::
elastic-filter-out = message.bank::
elastic-filter-out = eospromoter1::
elastic-filter-out = eospromotera::
elastic-filter-out = watchdoggiee::
elastic-filter-out = eoseosaddddd::
elastic-filter-out = eosblackdrop::

# load elasticsearch plugin
plugin = eosio::elasticsearch_plugin
...
```

## Resources:  
[Tune for Indexing Speed](https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-indexing-speed.html)  
[Tune For Disk Usage](https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-disk-usage.html)  
[How many shards should I have in my Elasticsearch cluster?](https://www.elastic.co/blog/how-many-shards-should-i-have-in-my-elasticsearch-cluster)