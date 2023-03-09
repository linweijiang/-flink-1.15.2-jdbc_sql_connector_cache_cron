# flink-1.15.2-jdbc_connector_cache_cron
#### JDBC SQL Connector基于Lookup Cache新增支持cron表达式的全量cache的功能

配置方式参考：
```sql
CREATE TABLE cs_list (
    idNo STRING,
    name String,
    age int
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.9.10:3306/test?characterEncoding=UTF-8',
    'table-name' = 'cs_list',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.all' = 'true',
    'lookup.cache.all.cron' = '0 * * * * ?',
    'lookup.cache.max-rows' = '123456',
    'lookup.cache.ttl' = '30s'
);
```
