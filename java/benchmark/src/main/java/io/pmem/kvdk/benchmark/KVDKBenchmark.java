/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

package io.pmem.kvdk.benchmark;

import io.pmem.kvdk.Configs;
import io.pmem.kvdk.Engine;
import io.pmem.kvdk.NativeBytesHandle;
import io.pmem.kvdk.benchmark.util.LongGenerator;
import io.pmem.kvdk.benchmark.util.RandomLongGenerator;
import io.pmem.kvdk.benchmark.util.RangeLongGenerator;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;
import java.util.Random;

public class KVDKBenchmark {
    private static final long MAX_VALUE_SIZE = 102400;
    private static final long MAX_LATENCY = 10000000;
    private static final long VALUE_RANDOM_SEED = 42;

    private String path;
    private long numKv;
    private long numOperations;
    private boolean fill;
    private long timeout;
    private long valueSize;
    private String valueSizeDistributionStr;
    private long threads;
    private double readRatio;
    private double existingKeysRatio;
    private boolean latency;
    private String type;
    private boolean scan;
    private long numCollection;
    private long batchSize;
    private String keyDistributionStr;
    private boolean populate;
    private long maxAccessThreads;
    private long space;
    private boolean optLargeSortedCollectionRestore;
    private boolean useDevdaxMode;

    Map<Flag, Object> flags;

    DataType dataType;
    KeyDistribution keyDistribution;
    ValueSizeDistribution valueSizeDistribution;

    Object finishLock;
    boolean isFinished;

    private List<Strig> collectionNames;
    private NativeBytesHandle[] collectionNameHandles;
    private long numOperationsPerThread;
    private long maxRandomKey;
    private List<LongGenerator> randomGenerators;
    private List<LongGenerator> rangeGenerators;
    private long writeThreads;
    private List<long[]> latencies;

    private Engine kvdkEngine;
    private List<AutoCloseable> closeableObjects;
    private String valuePool;

    public KVDKBenchmark(Map<Flag, Object> flags) {
        path = (String) flags.get(Flag.path);
        numKv = (Long) flags.get(Flag.num_kv);
        numOperations = (Long) flags.get(Flag.num_operations);
        fill = (Boolean) flags.get(Flag.fill);
        timeout = (Long) flags.get(Flag.timeout);
        valueSize = (Long) flags.get(Flag.value_size);
        valueSizeDistributionStr = (String) flags.get(Flag.value_size_distribution);
        threads = (Long) flags.get(Flag.threads);
        readRatio = (Double) flags.get(Flag.read_ratio);
        existingKeysRatio = (Double) flags.get(Flag.existing_keys_ratio);
        latency = (Boolean) flags.get(Flag.latency);
        type = (String) flags.get(Flag.type);
        scan = (Boolean) flags.get(Flag.scan);
        numCollection = (Long) flags.get(Flag.num_collection);
        batchSize = (Long) flags.get(Flag.batch_size);
        keyDistributionStr = (String) flags.get(Flag.key_distribution);
        populate = (Boolean) flags.get(Flag.populate);
        maxAccessThreads = (Long) flags.get(Flag.max_access_threads);
        space = (Long) flags.get(Flag.space);
        optLargeSortedCollectionRestore =
                (Boolean) flags.get(Flag.opt_large_sorted_collection_restore);
        useDevdaxMode = (Boolean) flags.get(Flag.use_devdax_mode);

        this.flags = flags;
        closeableObjects = new ArrayList<>();
        finishLock = new Object();
        isFinished = false;

        printFlags();
        processBenchmarkConfigs();
        generateValuePool();
    }

    private void printFlags() {
        System.out.println("flags:");
        for (Flag flag : Flag.values()) {
            System.out.format("  --%s=%s%n", flag.name(), flags.get(flag).toString());
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
            throw IllegalArgumentException("Unsupported data type: " + type);
        }

        if (dataType.equals(DataType.Sorted)) {
            collectionNames = new ArrayList<>();
            collectionNameHandles = new NativeBytesHandle[numCollection];
            for (int i = 0; i < numCollection; i++) {
                collectionNames.add("Collection_" + i);
            }
        }

        if (scan && dataType.equals(DataType.String)) {
            throw IllegalArgumentException("Unsupported to scan data type: " + type);
        }

        if (valueSize > MAX_VALUE_SIZE) {
            throw IllegalArgumentException(
                    "Value size is too large: "
                            + valueSize
                            + ", should be less than: "
                            + MAX_VALUE_SIZE);
        }

        if (readRatio < 0) {
            throw IllegalArgumentException("Invalid read_ratio: " + readRatio);
        }
        if (existingKeysRatio < 0) {
            throw IllegalArgumentException("Invalid existing_keys_ratio: " + existingKeysRatio);
        }
        maxRandomKey = existingKeysRatio == 0 ? Long.MAX_VALUE : numKv / existingKeysRatio;

        randomGenerators = new ArrayList<>();
        rangeGenerators = new ArrayList<>();
        if (fill) {
            assert (readRatio == 0);

            threads = maxAccessThreads; // overwrite
            System.out.println(
                    "Fill mode, num of threads is set to max_access_threads: " + maxAccessThreads);

            keyDistribution = KeyDistribution.Range;
            numOperationsPerThread = numKv / maxAccessThreads + 1;
            for (int i = 0; i < maxAccessThreads; i++) {
                rangeGenerators.add(
                        new RangeLongGenerator(
                                i * numOperationsPerThread, (i + 1) * numOperationsPerThread));
            }
        } else {
            numOperationsPerThread = numOperations / threads + 1;

            if (keyDistributionStr.equals("random")) {
                keyDistribution = KeyDistribution.Uniform;
            } else {
                throw IllegalArgumentException("Invalid key distribution: " + keyDistributionStr);
            }

            for (int i = 0; i < threads; i++) {
                randomGenerators.add(new RandomLongGenerator(0, maxRandomKey));
            }
        }

        if (valueSizeDistributionStr.equals("constant")) {
            valueSizeDistribution = ValueSizeDistribution.Constant;
        } else if (valueSizeDistributionStr.equals("random")) {
            valueSizeDistribution = ValueSizeDistribution.Uniform;
        } else {
            throw IllegalArgumentException("Invalid key distribution: " + valueSizeDistributionStr);
        }

        writeThreads = fill ? threads : threads - (readRatio * 100 * threads / 100);

        if (latency) {
            System.out.println("Latency stat is enabled.");
            for (int i = 0; i < threads; i++) {
                latencies.add(new long[MAX_LATENCY]);
            }
        }
    }

    private void initKVDKEngine() {
        Configs configs = new Configs();

        configs.setPopulatePMemSpace(populate);
        configs.setMaxAccessThreads(maxAccessThreads);
        configs.setPMemFileSize(space);
        configs.setOptLargeSortedCollectionRecovery(optLargeSortedCollectionRestore);
        configs.setUseDevDaxMode(useDevdaxMode);

        kvdkEngine = Engine.open(path, configs);

        configs.close();
    }

    private void generateValuePool() {
        StringBuilder sb = new StringBuilder();
        Random random = new Random(VALUE_RANDOM_SEED);
        int low = 'a';
        for (int i = 0; i < valueSize; i++) {
            sb.append((char) (low + random.nextInt(26)));
        }

        valuePool = sb.toString();
    }

    private void createSortedCollections() {
        System.out.format("Creating %ld Sorted Collections", numCollection);
        for (String collectionName : collectionNames) {
            NativeBytesHandle nameHandle = new NativeBytesHandle(collectionName.getBytes());
            collectionNameHandles[i] = nameHandle;
            closeableObjects.add(nameHandle);
            kvdkEngine.sortedCreate(nameHandle);
        }
        kvdkEngine.releaseAccessThread();
    }

    private void close() {
        for (AutoCloseable c : closeableObjects) {
            c.close();
        }
    }

    public void run() {
        if (!dataType.equals(DataType.Blackhole)) {
            initKVDKEngine();
        }

        if (dataType.equals(DataType.Sorted)) {
            createSortedCollections();
        }

        close();
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
        num_kv((1 << 30), "Number of KVs to place") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        num_operations(
                (1 << 30),
                "Number of total operations. Asserted to be equal to num_kv if \n"
                        + "\t(fill == true).") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        fill(false, "Fill num_kv uniform kv pairs to a new instance") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        timeout(30, "Time to benchmark, this is valid only if fill=false") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        value_size(120, "Value size of KV") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        value_size_distribution(
                "constant",
                "Distribution of value size to write, can be constant/random,\n"
                        + "\tdefault is constant. If set to random, the max value size\n"
                        + "\twill be --value_size.") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        threads(10, "Number of concurrent threads to run benchmark") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        read_ratio(0, "Read threads = threads * read_ratio") {
            @Override
            protected Object parseValue(String value) {
                return Double.parseDouble(value);
            }
        },
        existing_keys_ratio(
                1,
                "Ratio of keys to read / write that existed in the filled instance, for\n"
                        + "\texample, if set to\n"
                        + "\t1, all writes will be updates, and all read keys will be existed") {
            @Override
            protected Object parseValue(String value) {
                return Double.parseDouble(value);
            }
        },
        latency(false, "Stat operation latencies") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        type(
                "string",
                "Storage engine to benchmark, can be string, sorted, hash, list\n"
                        + "\tor blackhole") {
            @Override
            protected Object parseValue(String value) {
                return value;
            }
        },
        scan(
                false,
                "If set true, read threads will do scan operations, this is valid\n"
                        + "\tonly if we benchmark sorted or hash engine") {
            @Override
            protected Object parseValue(String value) {
                return parseBoolean(value);
            }
        },
        num_collection(1, "Number of collections in the instance to benchmark") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        batch_size(
                0,
                "Size of write batch. If batch>0, write string type kv with atomic batch\n"
                        + "\twrite, this is valid only if we benchmark string engine") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
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
        max_access_threads(32, "Max access threads of the instance") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
            }
        },
        space((256L << 30), "Max usable PMem space of the instance") {
            @Override
            protected Object parseValue(String value) {
                return Long.parseLong(value);
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

        protected abstract Object parseValue(String value);

        private final Object defaultValue;
        private final String description;
    }

    public static void printHelp() {
        System.out.println("usage:");
        for (Flag flag : Flag.values()) {
            System.out.format("  --%s%n\t%s%n", flag.name(), flag.getDescription());
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
            if (arg.equals("--help") || arg.equals("-h") || arg.equals("-?") || arg.equals("?")) {
                printHelp();
                System.exit(0);
            }
            if (arg.startsWith("--")) {
                try {
                    String[] parts = arg.substring(2).split("=");
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
                System.err.println("Invalid argument " + arg);
                System.err.println("Run with --help to show help information");
                System.exit(1);
            }
        }

        return flags;
    }

    public static void main(String[] args) {
        Map<Flag, Object> flags = parseArgs(args);
        new KVDKBenchmark(flags).run();
    }
}
