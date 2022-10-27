/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.benchmark;

import io.pmem.kvdk.Configs;
import io.pmem.kvdk.Engine;
import io.pmem.kvdk.Iterator;
import io.pmem.kvdk.KVDKException;
import io.pmem.kvdk.NativeBytesHandle;
import io.pmem.kvdk.WriteBatch;
import io.pmem.kvdk.benchmark.util.ConstantLongGenerator;
import io.pmem.kvdk.benchmark.util.LongGenerator;
import io.pmem.kvdk.benchmark.util.RandomLongGenerator;
import io.pmem.kvdk.benchmark.util.RangeLongGenerator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KVDKBenchmark {
    private static final long MAX_VALUE_SIZE = 102400;
    private static final long VALUE_RANDOM_SEED = 42;
    private static final int WARMUP_SECONDS = 2;
    private static final int MAX_LATENCY = 10000000; // Unit: 100 ns

    String path;
    long numKv;
    long numOperations;
    boolean fill;
    long timeout;
    long valueSize;
    String valueSizeDistributionStr;
    int threads;
    double readRatio;
    double existingKeysRatio;
    boolean latency;
    String type;
    boolean scan;
    int numCollection;
    long batchSize;
    String keyDistributionStr;
    boolean populate;
    int maxAccessThreads;
    long space;
    boolean optLargeSortedCollectionRestore;
    boolean useDevdaxMode;
    long hashBucketNum;

    Map<Flag, Object> flags;

    List<String> collectionNames;
    List<AutoCloseable> closeableObjects;
    long maxRandomKey;
    int writeThreads;
    int readThreads;
    ExecutorService executor;

    List<Long> readOpsLog;
    List<Long> writeOpsLog;
    List<Long> readNotFoundCountLog;
    int lastValidLogIndex;

    DataType dataType;
    KeyDistribution keyDistribution;
    ValueSizeDistribution valueSizeDistribution;

    AtomicLong finishedThreads;
    AtomicBoolean isTimeout;
    AtomicLong readOps;
    AtomicLong writeOps;
    AtomicLong readNotFoundCount;

    Engine kvdkEngine;
    byte[] valuePool;
    long numOperationsPerThread;
    NativeBytesHandle[] collectionNameHandles;
    LongGenerator[] randomKeyGenerators;
    LongGenerator[] rangeKeyGenerators;
    LongGenerator[] randomValueSizeGenerators;
    LongGenerator[] constantValueSizeGenerators;
    long[][] latencies;

    static {
        Engine.loadLibrary();
    }

    public KVDKBenchmark(Map<Flag, Object> flags) {
        path = (String) flags.get(Flag.path);
        numKv = (Long) flags.get(Flag.num_kv);
        numOperations = (Long) flags.get(Flag.num_operations);
        fill = (Boolean) flags.get(Flag.fill);
        timeout = (Long) flags.get(Flag.timeout);
        valueSize = (Long) flags.get(Flag.value_size);
        valueSizeDistributionStr = (String) flags.get(Flag.value_size_distribution);
        threads = ((Long) flags.get(Flag.threads)).intValue();
        readRatio = (Double) flags.get(Flag.read_ratio);
        existingKeysRatio = (Double) flags.get(Flag.existing_keys_ratio);
        latency = (Boolean) flags.get(Flag.latency);
        type = (String) flags.get(Flag.type);
        scan = (Boolean) flags.get(Flag.scan);
        numCollection = ((Long) flags.get(Flag.num_collection)).intValue();
        batchSize = (Long) flags.get(Flag.batch_size);
        keyDistributionStr = (String) flags.get(Flag.key_distribution);
        populate = (Boolean) flags.get(Flag.populate);
        maxAccessThreads = ((Long) flags.get(Flag.max_access_threads)).intValue();
        space = (Long) flags.get(Flag.space);
        optLargeSortedCollectionRestore =
                (Boolean) flags.get(Flag.opt_large_sorted_collection_restore);
        useDevdaxMode = (Boolean) flags.get(Flag.use_devdax_mode);
        hashBucketNum = (Long) flags.get(Flag.hash_bucket_num);

        this.flags = flags;
        closeableObjects = new ArrayList<>();
        finishedThreads = new AtomicLong();
        isTimeout = new AtomicBoolean(false);
        readOps = new AtomicLong();
        writeOps = new AtomicLong();
        readNotFoundCount = new AtomicLong();
        readOpsLog = new ArrayList<>();
        writeOpsLog = new ArrayList<>();
        readNotFoundCountLog = new ArrayList<>();

        printFlags();
        processBenchmarkConfigs();
        generateValuePool();
    }

    private void printFlags() {
        System.out.println("flags:");
        for (Flag flag : Flag.values()) {
            System.out.format("  -%s=%s%n", flag.name(), flags.get(flag).toString());
        }
    }

    private void processBenchmarkConfigs() {
        if (type.equals("sorted")) {
            dataType = DataType.Sorted;
        } else if (type.equals("string")) {
            dataType = DataType.String;
        } else if (type.equals("blackhole")) {
            dataType = DataType.Blackhole;
        } else {
            throw new IllegalArgumentException("Unsupported -type: " + type);
        }

        if (dataType.equals(DataType.Sorted)) {
            collectionNames = new ArrayList<>();
            collectionNameHandles = new NativeBytesHandle[numCollection];
            for (int i = 0; i < numCollection; i++) {
                collectionNames.add("Collection_" + i);
            }
        }

        if (scan && dataType.equals(DataType.String)) {
            throw new IllegalArgumentException("Unsupported to scan for -type: " + type);
        }

        if (valueSize > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException(
                    "-value_size is too large: "
                            + valueSize
                            + ", should be less than: "
                            + MAX_VALUE_SIZE);
        }

        maxRandomKey = existingKeysRatio == 0 ? Long.MAX_VALUE : (long) (numKv / existingKeysRatio);

        if (fill) {
            assert (readRatio == 0);
            System.out.println("Fill mode, -num_operations is ignored, will use -num_kv");

            if (threads > maxAccessThreads) {
                threads = maxAccessThreads; // overwrite
                System.out.println(
                        "Fill mode, -threads is reset to -max_access_threads: " + maxAccessThreads);
            }
            System.out.println("Fill mode, -threads: " + threads);
            System.out.println("Fill mode, -key_distribution is reset to: range");

            keyDistribution = KeyDistribution.Range;
            numOperationsPerThread = numKv / threads + 1;
            rangeKeyGenerators = new LongGenerator[threads];
            for (int i = 0; i < threads; i++) {
                rangeKeyGenerators[i] =
                        new RangeLongGenerator(
                                i * numOperationsPerThread, (i + 1) * numOperationsPerThread, 1);
            }
        } else {
            numOperationsPerThread = numOperations / threads + 1;

            if (keyDistributionStr.equals("random")) {
                keyDistribution = KeyDistribution.Uniform;
            } else {
                throw new IllegalArgumentException(
                        "Invalid -key_distribution: " + keyDistributionStr);
            }

            randomKeyGenerators = new LongGenerator[threads];
            for (int i = 0; i < threads; i++) {
                randomKeyGenerators[i] = new RandomLongGenerator(0, maxRandomKey);
            }
        }
        System.out.println("Num of operations per thread: " + numOperationsPerThread);

        if (valueSizeDistributionStr.equals("constant")) {
            valueSizeDistribution = ValueSizeDistribution.Constant;
            constantValueSizeGenerators = new LongGenerator[threads];
            for (int i = 0; i < threads; i++) {
                constantValueSizeGenerators[i] = new ConstantLongGenerator(valueSize);
            }
        } else if (valueSizeDistributionStr.equals("random")) {
            valueSizeDistribution = ValueSizeDistribution.Uniform;
            randomValueSizeGenerators = new LongGenerator[threads];
            for (int i = 0; i < threads; i++) {
                randomValueSizeGenerators[i] = new RandomLongGenerator(1, valueSize + 1);
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid -value_size_distribution: " + valueSizeDistributionStr);
        }

        writeThreads = fill ? threads : (int) (threads - (readRatio * 100 * threads / 100));
        readThreads = threads - writeThreads;

        if (latency) {
            System.out.println("Latency stat is enabled.");
            latencies = new long[threads][MAX_LATENCY];
        }
    }

    private void initKVDKEngine() throws KVDKException {
        Configs configs = new Configs();

        configs.setMaxAccessThreads(maxAccessThreads);
        configs.setOptLargeSortedCollectionRecovery(optLargeSortedCollectionRestore);
        configs.setUseDevDaxMode(useDevdaxMode);
        configs.setHashBucketNum(hashBucketNum);

        kvdkEngine = Engine.open(path, configs);

        configs.close();
        closeableObjects.add(kvdkEngine);
    }

    private void generateValuePool() {
        StringBuilder sb = new StringBuilder();
        Random random = new Random(VALUE_RANDOM_SEED);
        int low = 'a';
        for (int i = 0; i < valueSize; i++) {
            sb.append((char) (low + random.nextInt(26)));
        }

        valuePool = sb.toString().getBytes();
    }

    private void createSortedCollections() throws KVDKException {
        System.out.format("Creating %d Sorted Collections", numCollection);
        for (int i = 0; i < collectionNames.size(); i++) {
            String collectionName = collectionNames.get(i);
            NativeBytesHandle nameHandle = new NativeBytesHandle(collectionName.getBytes());
            collectionNameHandles[i] = nameHandle;
            closeableObjects.add(nameHandle);
            kvdkEngine.sortedCreate(nameHandle);
        }
    }

    private void startTasks() {
        System.out.println("Init " + readThreads + " readers and " + writeThreads + " writers.");
        executor = Executors.newCachedThreadPool();
        for (int i = 0; i < writeThreads; i++) {
            executor.submit(new WriteTask(i));
        }
        for (int i = writeThreads; i < threads; i++) {
            Callable<Void> task = scan ? new ScanTask(i) : new ReadTask(i);
            executor.submit(task);
        }
    }

    private void shutdownTasks() throws InterruptedException {
        System.out.println("Shutdown task executor.");
        executor.shutdown();
        boolean finished = executor.awaitTermination(10, TimeUnit.SECONDS);
        if (!finished) {
            System.out.println("Executor was not finished after 10 seconds. Force shutdown.");
            executor.shutdownNow();
        }
        System.out.println("Executor was shut down.");
    }

    private void printProgress() throws InterruptedException {
        System.out.println("----------------------------------------------------------");
        System.out.format(
                "%-15s%-15s%-15s%-15s%-15s%-15s\n",
                "Time(ms)", "Read Ops", "Write Ops", "Not Found", "Total Read", "Total Write");
        long start = System.currentTimeMillis();

        readOpsLog.add(Long.valueOf(0));
        writeOpsLog.add(Long.valueOf(0));
        readNotFoundCountLog.add(Long.valueOf(0));
        lastValidLogIndex = 0;
        while (true) {
            Thread.sleep(1000);
            long duration = System.currentTimeMillis() - start;

            readOpsLog.add(readOps.get());
            writeOpsLog.add(writeOps.get());
            readNotFoundCountLog.add(readNotFoundCount.get());

            int idx = readOpsLog.size() - 1;
            System.out.format(
                    "%-15d%-15d%-15d%-15d%-15d%-15d\n",
                    duration,
                    readOpsLog.get(idx) - readOpsLog.get(idx - 1),
                    writeOpsLog.get(idx) - writeOpsLog.get(idx - 1),
                    readNotFoundCountLog.get(idx) - readNotFoundCountLog.get(idx - 1),
                    readOpsLog.get(idx),
                    writeOpsLog.get(idx));

            if (finishedThreads.get() == 0 || idx <= WARMUP_SECONDS) {
                lastValidLogIndex = idx;
            }

            if (finishedThreads.get() == threads) {
                break;
            }

            if (!fill && duration >= timeout * 1000) {
                // Signal time out
                System.out.println("Benchmark timeout (second): " + timeout);
                isTimeout.set(true);
                break;
            }
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Benchmark finished, duration (ms): " + duration);
    }

    private void printLatency() {
        long ro = readOps.get();
        if (ro > 0 && readThreads > 0 && !scan) {
            double avg = 0;
            double l50 = 0;
            double l99 = 0;
            double l995 = 0;
            double l999 = 0;
            double l9999 = 0;

            long accumulatedOps = 0;
            long accumulatedLatency = 0;
            for (int i = 1; i < MAX_LATENCY; i++) {
                for (int j = 0; j < readThreads; j++) {
                    accumulatedOps += latencies[writeThreads + j][i];
                    accumulatedLatency += latencies[writeThreads + j][i] * i;

                    if (l50 == 0 && (double) accumulatedOps / ro > 0.5) {
                        l50 = (double) i / 10;
                    } else if (l99 == 0 && (double) accumulatedOps / ro > 0.99) {
                        l99 = (double) i / 10;
                    } else if (l995 == 0 && (double) accumulatedOps / ro > 0.995) {
                        l995 = (double) i / 10;
                    } else if (l999 == 0 && (double) accumulatedOps / ro > 0.999) {
                        l999 = (double) i / 10;
                    } else if (l9999 == 0 && (double) accumulatedOps / ro > 0.9999) {
                        l9999 = (double) i / 10;
                    }
                }
            }
            avg = accumulatedLatency / ro / 10;

            System.out.format(
                    "read lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, "
                            + "P99.5: %.2f, P99.9: %.2f, P99.99: %.2f\n",
                    avg, l50, l99, l995, l999, l9999);
        }

        long wo = writeOps.get();
        if (wo > 0 && writeThreads > 0) {
            double avg = 0;
            double l50 = 0;
            double l99 = 0;
            double l995 = 0;
            double l999 = 0;
            double l9999 = 0;

            long accumulatedOps = 0;
            long accumulatedLatency = 0;
            for (int i = 1; i < MAX_LATENCY; i++) {
                for (int j = 0; j < writeThreads; j++) {
                    accumulatedOps += latencies[j][i];
                    accumulatedLatency += latencies[j][i] * i;

                    if (l50 == 0 && (double) accumulatedOps / wo > 0.5) {
                        l50 = (double) i / 10;
                    } else if (l99 == 0 && (double) accumulatedOps / wo > 0.99) {
                        l99 = (double) i / 10;
                    } else if (l995 == 0 && (double) accumulatedOps / wo > 0.995) {
                        l995 = (double) i / 10;
                    } else if (l999 == 0 && (double) accumulatedOps / wo > 0.999) {
                        l999 = (double) i / 10;
                    } else if (l9999 == 0 && (double) accumulatedOps / wo > 0.9999) {
                        l9999 = (double) i / 10;
                    }
                }
            }
            avg = accumulatedLatency / wo / 10;

            System.out.format(
                    "write lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, "
                            + "P99.5: %.2f, P99.9: %.2f, P99.99: %.2f\n",
                    avg, l50, l99, l995, l999, l9999);
        }
    }

    private void printFinalStats() {
        int timeElapsedInSeconds = 0;
        long totalValidRead = 0;
        long totalValidWrite = 0;

        if (lastValidLogIndex <= WARMUP_SECONDS) {
            timeElapsedInSeconds = lastValidLogIndex;
            totalValidRead = readOpsLog.get(lastValidLogIndex);
            totalValidWrite = writeOpsLog.get(lastValidLogIndex);
        } else {
            timeElapsedInSeconds = lastValidLogIndex - WARMUP_SECONDS;
            totalValidRead = readOpsLog.get(lastValidLogIndex) - readOpsLog.get(WARMUP_SECONDS);
            totalValidWrite = writeOpsLog.get(lastValidLogIndex) - writeOpsLog.get(WARMUP_SECONDS);
        }

        if (timeElapsedInSeconds == 0) {
            return;
        }

        System.out.println("----------------------------------------------------------");
        System.out.println(
                "Average Read Ops:\t"
                        + totalValidRead / timeElapsedInSeconds
                        + ". Average Write Ops:\t"
                        + totalValidWrite / timeElapsedInSeconds);

        if (latency) {
            printLatency();
        }
    }

    private void closeObjects() throws Exception {
        Collections.reverse(closeableObjects);
        for (AutoCloseable c : closeableObjects) {
            c.close();
        }
    }

    public void run() throws Exception {
        if (!dataType.equals(DataType.Blackhole)) {
            initKVDKEngine();
        }

        if (dataType.equals(DataType.Sorted)) {
            createSortedCollections();
        }

        startTasks();

        try {
            printProgress();
        } finally {
            shutdownTasks();

            printFinalStats();

            closeObjects();
        }
    }

    private enum DataType {
        String,
        Sorted,
        Blackhole;
    }

    private enum KeyDistribution {
        Range,
        Uniform
    }

    private enum ValueSizeDistribution {
        Constant,
        Uniform
    }

    private enum Flag {
        path("/mnt/pmem0/kvdk", "Instance path") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        num_kv((1L << 30), "Number of KVs to place") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("num_kv", value);
            }
        },
        num_operations(
                (1L << 30),
                "Number of total operations. Asserted to be equal to num_kv if \n"
                        + "\t(fill == true).") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("num_operations", value);
            }
        },
        fill(false, "Fill num_kv uniform kv pairs to a new instance") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        timeout(30L, "Time (seconds) to benchmark, this is valid only if fill=false") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("timeout", value);
            }
        },
        value_size(120L, "Value size of KV") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("value_size", value);
            }
        },
        value_size_distribution(
                "constant",
                "Distribution of value size to write, can be constant/random,\n"
                        + "\tdefault is constant. If set to random, the max value size\n"
                        + "\twill be -value_size.") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        threads(10L, "Number of concurrent threads to run benchmark") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("threads", value);
            }
        },
        read_ratio(0.0, "Read threads = threads * read_ratio") {
            @Override
            protected Object parseValue(String value) {
                return parseDoubleRatio("read_ratio", value);
            }
        },
        existing_keys_ratio(
                1.0,
                "Ratio of keys to read / write that existed in the filled instance, for\n"
                        + "\texample, if set to\n"
                        + "\t1, all writes will be updates, and all read keys will be existed") {
            @Override
            protected Object parseValue(String value) {
                return parseDoubleRatio("existing_keys_ratio", value);
            }
        },
        latency(false, "Stat operation latencies") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        type("string", "Storage engine to benchmark, can be string, sorted or blackhole") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        scan(
                false,
                "If set true, read threads will do scan operations, this is valid\n"
                        + "\tonly if we benchmark sorted engine") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        num_collection(1L, "Number of collections in the instance to benchmark") {
            @Override
            protected Object parseValue(String value) {
                return parseLongNotNegtive("num_collection", value);
            }
        },
        batch_size(
                0L,
                "Size of write batch. If batch>0, write string type kv with atomic batch\n"
                        + "\twrite, this is valid only if we benchmark string engine") {
            @Override
            protected Object parseValue(String value) {
                return parseLongNotNegtive("batch_size", value);
            }
        },
        key_distribution(
                "random",
                "Distribution of benchmark keys, if fill is true, this param will\n"
                        + "\tbe ignored and only uniform distribution will be used") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        populate(
                false,
                "Populate pmem space while creating a new instance. This can improve write\n"
                        + "\tperformance in runtime, but will take long time to init the instance") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        max_access_threads(32L, "Max access threads of the instance") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("max_access_threads", value);
            }
        },
        space((256L << 30), "Max usable PMem space of the instance") {
            @Override
            protected Object parseValue(String value) {
                return parseLongPositive("space", value);
            }
        },
        opt_large_sorted_collection_restore(
                true,
                "Optional optimization strategy which Multi-thread recovery a\n"
                        + "\tskiplist. When having few large skiplists, the optimization can\n"
                        + "\tget better performance") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        use_devdax_mode(false, "Use devdax device for kvdk") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        hash_bucket_num(
                (1L << 27),
                "The number of bucket groups in the hash table.\n"
                        + "\tIt should be 2^n and should smaller than 2^32.") {
            @Override
            protected Object parseValue(String value) {
                Long ret = parseLongPositive("hash_bucket_num", value);
                if (!isPowerOfTwo(ret) || (int) (Math.ceil((Math.log(ret) / Math.log(2)))) >= 32) {
                    throw new IllegalArgumentException(
                            "Invalid value for -hash_bucket_num: " + ret);
                }
                return ret;
            }
        };

        private Flag(Object defaultValue, String description) {
            this.defaultValue = defaultValue;
            this.description = description;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public String getDescription() {
            return description;
        }

        public boolean parseBoolean(String value) {
            if (value.equals("1")) {
                return true;
            } else if (value.equals("0")) {
                return false;
            }
            return Boolean.parseBoolean(value);
        }

        public Long parseLongNotNegtive(String name, String value) {
            Long ret = Long.parseLong(value);
            if (ret < 0) {
                throw new IllegalArgumentException("Invalid value for -" + name + ": " + ret);
            }
            return ret;
        }

        public Long parseLongPositive(String name, String value) {
            Long ret = Long.parseLong(value);
            if (ret <= 0) {
                throw new IllegalArgumentException("Invalid value for -" + name + ": " + ret);
            }
            return ret;
        }

        public Double parseDoubleRatio(String name, String value) {
            Double ret = Double.parseDouble(value);
            if (ret < 0 || ret > 1) {
                throw new IllegalArgumentException("Invalid value for -" + name + ": " + ret);
            }
            return ret;
        }

        static boolean isPowerOfTwo(long n) {
            return (int) (Math.ceil((Math.log(n) / Math.log(2))))
                    == (int) (Math.floor(((Math.log(n) / Math.log(2)))));
        }

        protected abstract Object parseValue(String value);

        private final Object defaultValue;
        private final String description;
    }

    abstract class BenchmarkTask implements Callable<Void> {
        protected int tid;

        public BenchmarkTask(int tid) {
            this.tid = tid;
        }

        @Override
        public Void call() throws Exception {
            run();
            return null;
        }

        protected abstract void run() throws KVDKException;

        protected long generateKey() {
            switch (keyDistribution) {
                case Range:
                    return rangeKeyGenerators[tid].gen();
                case Uniform:
                    return randomKeyGenerators[tid].gen();
                default:
                    throw new UnsupportedOperationException();
            }
        }

        protected long generateValueSize() {
            switch (valueSizeDistribution) {
                case Constant:
                    return constantValueSizeGenerators[tid].gen();
                case Uniform:
                    return randomValueSizeGenerators[tid].gen();
                default:
                    throw new UnsupportedOperationException();
            }
        }

        public byte[] longToBytes(long x) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(x);
            return buffer.array();
        }
    }

    private class WriteTask extends BenchmarkTask {
        public WriteTask(int tid) {
            super(tid);
        }

        @Override
        public void run() throws KVDKException {
            WriteBatch batch = null;
            if (kvdkEngine != null) {
                batch = kvdkEngine.writeBatchCreate();
            }

            for (long operations = 0; operations < numOperationsPerThread; operations++) {
                if (isTimeout.get()) {
                    break;
                }

                long num = generateKey();
                byte[] key = longToBytes(num);
                int generatedValueSize = (int) generateValueSize();

                long start = 0;
                if (latency) {
                    start = System.nanoTime();
                }

                switch (dataType) {
                    case String:
                        if (batchSize == 0) {
                            kvdkEngine.put(key, 0, key.length, valuePool, 0, generatedValueSize);
                        } else {
                            batch.stringPut(key, 0, key.length, valuePool, 0, generatedValueSize);
                            if ((operations + 1) % batchSize == 0) {
                                kvdkEngine.batchWrite(batch);
                                batch.clear();
                            }
                        }
                        break;
                    case Sorted:
                        int cid = (int) (num % numCollection);
                        if (batchSize == 0) {
                            kvdkEngine.sortedPut(
                                    collectionNameHandles[cid],
                                    key,
                                    0,
                                    key.length,
                                    valuePool,
                                    0,
                                    generatedValueSize);
                        } else {
                            batch.sortedPut(
                                    collectionNameHandles[cid],
                                    key,
                                    0,
                                    key.length,
                                    valuePool,
                                    0,
                                    generatedValueSize);
                            if ((operations + 1) % batchSize == 0) {
                                kvdkEngine.batchWrite(batch);
                                batch.clear();
                            }
                        }
                        break;
                    case Blackhole:
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                if (latency) {
                    long lat = System.nanoTime() - start;
                    if (lat / 100 >= MAX_LATENCY) {
                        throw new RuntimeException("Write latency overflow");
                    }
                    latencies[tid][(int) (lat / 100)]++;
                }

                if ((operations + 1) % 1000 == 0) {
                    writeOps.addAndGet(1000);
                }
            }

            if (batch != null) {
                batch.close();
            }

            finishedThreads.incrementAndGet();
        }
    }

    private class ReadTask extends BenchmarkTask {
        public ReadTask(int tid) {
            super(tid);
        }

        @Override
        public void run() throws KVDKException {
            long readNotFound = 0;
            for (long operations = 0; operations < numOperationsPerThread; operations++) {
                if (isTimeout.get()) {
                    break;
                }

                long num = generateKey();
                byte[] key = longToBytes(num);
                byte[] value = null;

                long start = 0;
                if (latency) {
                    start = System.nanoTime();
                }

                switch (dataType) {
                    case String:
                        value = kvdkEngine.get(key);
                        break;
                    case Sorted:
                        int cid = (int) (num % numCollection);
                        value = kvdkEngine.sortedGet(collectionNameHandles[cid], key);
                        break;
                    case Blackhole:
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                if (latency) {
                    long lat = System.nanoTime() - start;
                    if (lat / 100 >= MAX_LATENCY) {
                        throw new RuntimeException("Read latency overflow");
                    }
                    latencies[tid][(int) (lat / 100)]++;
                }

                if (value == null && !dataType.equals(DataType.Blackhole)) {
                    // Not found
                    if (++readNotFound % 1000 == 0) {
                        readNotFoundCount.addAndGet(1000);
                    }
                }

                if ((operations + 1) % 1000 == 0) {
                    readOps.addAndGet(1000);
                }
            }

            finishedThreads.incrementAndGet();
        }
    }

    private class ScanTask extends BenchmarkTask {
        public ScanTask(int tid) {
            super(tid);
        }

        @Override
        public void run() throws KVDKException {
            long readNotFound = 0;
            for (long operations = 0, operations_counted = 0;
                    operations < numOperationsPerThread; ) {
                if (isTimeout.get()) {
                    break;
                }

                long num = generateKey();
                byte[] key = longToBytes(num);
                byte[] value = null;

                final int scanLength = 100;
                switch (dataType) {
                    case Sorted:
                        int cid = (int) (num % numCollection);
                        Iterator iter = kvdkEngine.sortedIteratorCreate(collectionNameHandles[cid]);
                        iter.seek(key);
                        for (int i = 0; i < scanLength && iter.isValid(); i++, iter.next()) {
                            key = iter.key();
                            value = iter.value();

                            operations++;
                            if (operations >= operations_counted + 1000) {
                                readOps.addAndGet(operations - operations_counted);
                                operations_counted = operations;
                            }
                        }
                        kvdkEngine.sortedIteratorRelease(iter);

                        break;
                    case Blackhole:
                        operations += 1024;
                        readOps.addAndGet(1024);
                        break;
                    case String:
                    default:
                        throw new UnsupportedOperationException();
                }

                if ((operations + 1) % 1000 == 0) {
                    writeOps.addAndGet(1000);
                }
            }

            finishedThreads.incrementAndGet();
        }
    }

    public static void printHelp() {
        System.out.println("usage:");
        for (Flag flag : Flag.values()) {
            System.out.format("  -%s%n\t%s%n", flag.name(), flag.getDescription());
            if (flag.getDefaultValue() != null) {
                System.out.format("\tDefault: %s%n", flag.getDefaultValue().toString());
            }
        }
    }

    private static Map<Flag, Object> parseArgs(String[] args) {
        Map<Flag, Object> flags = new EnumMap<Flag, Object>(Flag.class);
        for (Flag flag : Flag.values()) {
            if (flag.getDefaultValue() != null) {
                flags.put(flag, flag.getDefaultValue());
            }
        }

        for (String arg : args) {
            boolean valid = false;
            if (arg.equals("-h") || arg.equals("--help") || arg.equals("-?") || arg.equals("?")) {
                printHelp();
                System.exit(0);
            }
            if (arg.startsWith("-")) {
                try {
                    String[] parts = arg.substring(1).split("=");
                    if (parts.length >= 1) {
                        Flag key = Flag.valueOf(parts[0]);
                        if (key != null) {
                            Object value = null;
                            if (parts.length >= 2) {
                                value = key.parseValue(parts[1]);
                            }
                            flags.put(key, value);
                            valid = true;
                        }
                    }
                } catch (Exception e) {
                }
            }
            if (!valid) {
                System.err.println("Invalid argument: " + arg);
                System.err.println("Run with -h to show help information");
                System.exit(1);
            }
        }

        return flags;
    }

    public static void main(String[] args) throws Exception {
        Map<Flag, Object> flags = parseArgs(args);
        new KVDKBenchmark(flags).run();
    }
}
