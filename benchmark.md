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
    --replay-blockchain \
    --elastic-url=http://localhost:9200/ \
    --elastic-queue-size=512 \
    --elastic-abi-cache-size=8192 \
    --elastic-index-wipe

./build/programs/nodeos/nodeos --data-dir=dev_config/data \
    --config-dir=dev_config \
    --replay-blockchain \
    --mongodb-uri=mongodb://root:example@localhost:27017/eos?authSource=admin \
    --mongodb-queue-size=512 \
    --mongodb-abi-cache-size=8192 \
    --mongodb-wipe
```

## Replay 10000 Block

### Comparison

|                      | blocks | elapse(s) | speed(b/s) |
| -------------------- |:------:|:---------:|:----------:|
| mongo_db_plugin      | 10000  | 694       | 14.41      |
| elasticsearch_plugin | 10000  | 1568      | 6.38       |
