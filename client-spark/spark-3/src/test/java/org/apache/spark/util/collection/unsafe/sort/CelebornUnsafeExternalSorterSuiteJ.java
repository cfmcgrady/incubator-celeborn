/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.spark.util.collection.unsafe.sort;
//
// import org.apache.spark.SparkConf;
// import org.apache.spark.TaskContext;
// import org.apache.spark.executor.TaskMetrics;
// import org.apache.spark.memory.TaskMemoryManager;
// import org.apache.spark.memory.TestMemoryManager;
// import org.apache.spark.internal.config.package$;
// import org.apache.spark.serializer.JavaSerializer;
// import org.apache.spark.serializer.SerializerManager;
// import org.apache.spark.unsafe.Platform;
// import org.junit.Before;
// import org.junit.Test;
// import org.mockito.Mock;
//
// import java.io.IOException;
//
// import static org.mockito.Answers.RETURNS_SMART_NULLS;
// import static org.mockito.Mockito.mock;
// import static org.mockito.Mockito.when;
//
//// import static org.junit.jupiter.api.Assertions.*;
//
//
// public class CelebornUnsafeExternalSorterSuiteJ {
//
//    private final SparkConf conf = new SparkConf();
//
////    final LinkedList<File> spillFilesCreated = new LinkedList<>();
//    final TestMemoryManager memoryManager =
//            new TestMemoryManager(conf.clone().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(),
// false));
//    final TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
//    final SerializerManager serializerManager = new SerializerManager(
//            new JavaSerializer(conf),
//            conf.clone().set(package$.MODULE$.SHUFFLE_SPILL_COMPRESS(), false));
//    // Use integer comparison for comparing prefixes (which are partition ids, in this case)
//    final PrefixComparator prefixComparator = PrefixComparators.LONG;
//    // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
//    // use a dummy comparator
//    final RecordComparator recordComparator = new RecordComparator() {
//        @Override
//        public int compare(
//                Object leftBaseObject,
//                long leftBaseOffset,
//                int leftBaseLength,
//                Object rightBaseObject,
//                long rightBaseOffset,
//                int rightBaseLength) {
//            return 0;
//        }
//    };
//
////    @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
////    @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
//    @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;
//
//    protected boolean shouldUseRadixSort() { return false; }
//
//    private final long pageSizeBytes = conf.getSizeAsBytes(
//            package$.MODULE$.BUFFER_PAGESIZE().key(), "4m");
//
//    private final int spillThreshold =
//            (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
//
//    @Before
//    public void setUp() throws Exception {
////        MockitoAnnotations.openMocks(this).close();
////        tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "unsafe-test");
////        spillFilesCreated.clear();
//        taskContext = mock(TaskContext.class);
//        when(taskContext.taskMetrics()).thenReturn(new TaskMetrics());
////        when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
////        when(diskBlockManager.createTempLocalBlock()).thenAnswer(invocationOnMock -> {
////            TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
////            File file = File.createTempFile("spillFile", ".spill", tempDir);
////            spillFilesCreated.add(file);
////            return Tuple2$.MODULE$.apply(blockId, file);
////        });
////        when(blockManager.getDiskWriter(
////                any(BlockId.class),
////                any(File.class),
////                any(SerializerInstance.class),
////                anyInt(),
////                any(ShuffleWriteMetrics.class))).thenAnswer(invocationOnMock -> {
////            Object[] args = invocationOnMock.getArguments();
////
////            return new DiskBlockObjectWriter(
////                    (File) args[1],
////                    serializerManager,
////                    (SerializerInstance) args[2],
////                    (Integer) args[3],
////                    false,
////                    (ShuffleWriteMetrics) args[4],
////                    (BlockId) args[0]
////            );
////        });
//    }
//
//    private static void insertNumber(CelebornUnsafeExternalSorter sorter, int value) throws
// Exception {
//        final int[] arr = new int[]{ value };
//        sorter.insertRecord(arr, Platform.INT_ARRAY_OFFSET, 4, value, false);
//    }
//
//    private static void insertRecord(
//            CelebornUnsafeExternalSorter sorter,
//            int[] record,
//            long prefix) throws IOException {
//        sorter.insertRecord(record, Platform.INT_ARRAY_OFFSET, record.length * 4, prefix, false);
//    }
//
//    @Test
//    public void aa() throws Exception {
//        CelebornUnsafeExternalSorter sorter = CelebornUnsafeExternalSorter.create(
//                taskMemoryManager, null, null, taskContext,
//                () -> recordComparator,
//                prefixComparator,
//                /* initialSize */ 1024,
//                pageSizeBytes,
//                spillThreshold,
//                shouldUseRadixSort());
//
//        insertNumber(sorter, 5);
//        insertNumber(sorter, 1);
//        insertNumber(sorter, 3);
//        sorter.spill();
//        insertNumber(sorter, 4);
//        sorter.spill();
//        insertNumber(sorter, 2);
//
////        UnsafeSorterIterator iter = sorter.getSortedIterator();
//
////        for (int i = 1; i <= 5; i++) {
////            iter.loadNext();
////            assertEquals(i, iter.getKeyPrefix());
////            assertEquals(4, iter.getRecordLength());
////            assertEquals(i, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
////        }
//    }
//
////    @AfterEach
////    public void tearDown() {
////        try {
////            assertEquals(0L, taskMemoryManager.cleanUpAllAllocatedMemory());
////        } finally {
////            Utils.deleteRecursively(tempDir);
////            tempDir = null;
////        }
////    }
// }
