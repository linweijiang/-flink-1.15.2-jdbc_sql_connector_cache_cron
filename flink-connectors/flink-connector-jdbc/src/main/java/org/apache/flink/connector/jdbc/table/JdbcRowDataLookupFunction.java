/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.utils.CronUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link JdbcDynamicTableSource}. */
@Internal
public class JdbcRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupFunction.class);
    private static final long serialVersionUID = 2L;

    private final String query;
    private final JdbcConnectionProvider connectionProvider;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cacheMissingKey;
    private final boolean cacheAll;
    private final String cacheAllCron;
    private long lookupCacheLine;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private transient FieldNamedPreparedStatement statement;
    private transient Cache<RowData, List<RowData>> cache;

    public JdbcRowDataLookupFunction(
            JdbcConnectorOptions options,
            JdbcLookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.connectionProvider = new SimpleJdbcConnectionProvider(options);
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.cacheMissingKey = lookupOptions.getCacheMissingKey();
        this.cacheAll = lookupOptions.isCacheAll();
        this.cacheAllCron = lookupOptions.getCacheAllCron();
        this.query =
                lookupOptions.isCacheAll()
                        ? options.getDialect()
                                .getSelectFromStatementWithNoWhere(
                                        options.getTableName(), fieldNames)
                        : options.getDialect()
                                .getSelectFromStatement(
                                        options.getTableName(), fieldNames, keyNames);
        String dbURL = options.getDbURL();
        this.jdbcDialect = JdbcDialectLoader.load(dbURL);
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter =
                jdbcDialect.getRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            establishConnectionAndStatement();
            if (cacheAll) {
                initCacheAll();
                CronUtils.runCron(this::initCacheAll, cacheAllCron);
            } else {
                this.cache =
                        cacheMaxSize == -1 || cacheExpireMs == -1
                                ? null
                                : CacheBuilder.newBuilder()
                                        .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                        .maximumSize(cacheMaxSize)
                                        .build();
            }
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
        context.getMetricGroup()
                .gauge("Jdbc_Lookup_Cache_Size", (Gauge<Long>) this::getLookupCacheLine);
    }

    private synchronized void initCacheAll() {
        Cache<RowData, List<RowData>> initCache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheMaxSize == -1 ? Integer.MAX_VALUE : cacheMaxSize)
                        .build();
        try {
            // 初始化数据
            ResultSet resultSet = statement.executeQuery();
            long line = 0L;
            while (resultSet.next()) {
                // 生成对应的key
                RowData key = jdbcRowConverter.toInternal(keyNames, resultSet);
                // 获取对应的数据
                RowData row = jdbcRowConverter.toInternal(resultSet);
                // 保存到cache
                List<RowData> dataList = initCache.getIfPresent(key);
                if (dataList == null) {
                    dataList = new ArrayList<>();
                }
                dataList.add(row);
                ((ArrayList<?>) dataList).trimToSize();
                initCache.put(key, dataList);
                line++;
            }
            LOG.info("init all cache, size is: {} line", line);
            this.lookupCacheLine = line;
        } catch (Exception e) {
            LOG.error("init cache all data error, please check.", e);
        }
        this.cache = initCache;
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        if (cacheAll) {
            lookupWithAll(keys);
        } else {
            lookup(keys);
        }
    }

    public void lookupWithAll(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        List<RowData> rows = cache.getIfPresent(keyRow);
        if (rows != null) {
            for (RowData row : rows) {
                collect(row);
            }
        }
    }

    public void lookup(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (cache == null) {
                        while (resultSet.next()) {
                            collect(jdbcRowConverter.toInternal(resultSet));
                        }
                    } else {
                        ArrayList<RowData> rows = new ArrayList<>();
                        while (resultSet.next()) {
                            RowData row = jdbcRowConverter.toInternal(resultSet);
                            rows.add(row);
                            collect(row);
                        }
                        rows.trimToSize();
                        if (!rows.isEmpty() || cacheMissingKey) {
                            cache.put(keyRow, rows);
                        }
                    }
                }
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed",
                            exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    public long getLookupCacheLine() {
        return lookupCacheLine;
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection dbConn = connectionProvider.getOrEstablishConnection();
        statement = FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        connectionProvider.closeConnection();
    }

    @VisibleForTesting
    public Connection getDbConnection() {
        return connectionProvider.getConnection();
    }

    @VisibleForTesting
    public Cache<RowData, List<RowData>> getCache() {
        return cache;
    }
}
