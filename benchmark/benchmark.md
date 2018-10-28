# Benchmark

| Replay 10000 Block   | elapse(s) | speed(b/s) |
| -------------------- |:---------:|:----------:|
| elasticsearch_plugin | 266       | 37.59      |
| mongo_db_plugin      | 694       | 14.41      |

| Replay 100000 Block  | elapse(s) | speed(b/s) |
| -------------------- |:---------:|:----------:|
| elasticsearch_plugin | 354       | 282.49     |
| mongo_db_plugin      | 987       | 101.32     |

## Hardware

```text
Processor: AMD® Ryzen 5 1600 six-core processor × 12
Memory:    2 × 8GB DDR4 2400
Drive:     Kingston A1000 NVMe PCIe SSD 240GB
System:    Ubuntu 18.04
```

## Configuration

The MongoDB and Elasticsearch are both running in the docker container with basic config. see:  
[mongo-stack/docker-compose.yml](./mongo-stack/docker-compose.yml)  
[elastic-stack/docker-compose.yml](./elastic-stack/docker-compose.yml)

### Command-line

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
