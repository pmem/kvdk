### Clone And Build redis

run command `bash build_redis_kvdk.sh` in kvdk/examples/kvredis dictory.  

### Start Redis Server

Like `redis/redis.conf`, users can modify the config args of kvdk module. How to  
set config, please see https://github.com/pmem/kvdk/blob/main/doc/user_doc.md

### Bench Redis With Kvdk 
After started redis server, users can run command  
`bash bench_redis_with_kvdk.sh -h xxx.xxx.xxx.xxx -p port`  
for example:  
`bash bench_redis_with_kvdk.sh -h 192.168.1.3 -p 6380`  
If want to redis default ip address and post, you can directly  
`bash bench_redis_with_kvdk.sh`
