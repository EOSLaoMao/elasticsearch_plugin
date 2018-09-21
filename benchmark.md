# Benchmark

## Hardware

```text
Processor: AMD® Ryzen 5 1600 six-core processor × 12
Memory:    2 × 8GB DDR4 2400
```

## Configuration

```bash
./build/programs/nodeos/nodeos --data-dir=dev_config/data \
    --config-dir=dev_config \
    --hard-replay-blockchain \
    --elastic-url=http://localhost:9200/ \
    --elastic-queue-size=128 \
    --elastic-index-wipe

./build/programs/nodeos/nodeos --data-dir=dev_config/data \
    --config-dir=dev_config \
    --hard-replay-blockchain \
    --mongodb-uri=mongodb://root:example@localhost:27017/eos?authSource=admin \
    --mongodb-queue-size=128 \
    --mongodb-wipe
```

## Hard Replay 3000 Block

### Comparison

|                      | blocks | docs   | elapse(s) | speed(b/s) | docs/block |
| -------------------- |:------:|:------:|:---------:|:----------:|:----------:|
| mongo_db_plugin      | 3000   | 898361 | 911       | 3.3        | 299.45     |
| elasticsearch_plugin | 3000   | 818738 | 1200      | 2.5        | 272.91     |

### Process Fucntions Time Consuming

| mongo_db_plugin                 | average time(microsecond) |
| ------------------------------- |:-------------------------:|
| `_process_applied_transaction`  | 84472                     |
| `_process_accepted_transaction` | 12424                     |
| `_process_accepted_block`       | 15733                     |
| `_process_irreversible_block`   | 1539                      |

| elasticsearch_plugin            | average time(microsecond) |
| ------------------------------- |:-------------------------:|
| `_process_applied_transaction`  | 201891                    |
| `_process_accepted_transaction` | 17450                     |
| `_process_accepted_block`       | 35932                     |
| `_process_irreversible_block`   | 20958                     |
