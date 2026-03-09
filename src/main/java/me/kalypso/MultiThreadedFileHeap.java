package me.kalypso;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MultiThreadedFileHeap {

    public final ExecutorService ioWorker = Executors.newFixedThreadPool(4);
    private DumpTruck dumpTruck;
    private AtomicBoolean valid = new AtomicBoolean(true);

    /* Jackson JSON stuff */
    public static final ObjectMapper mapper = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static final JsonFactory factory = mapper.getFactory()
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);

    private final Path location;
    private final Path header, tempHeader;
    private final File headerFile;
    private final FileChannel channel;
    private final String name;

    /* The slack rate is used to expand the file by a fixed sized
     * Should be adjusted by creator of heap to optimize for type of item storage
     *
     * note for later, put this in constructor */
    private final int slackRate = 4*1024*1024;
    /* The remaining slack space */
    private AtomicInteger slackSpace = new AtomicInteger();
    /* The offset of where a completely new free allocation can be made within slack space */
    private AtomicLong nextOffset = new AtomicLong();

    private MultiThreadedFileHeap(Path location, String name, Path header, FileChannel channel) {
        this.location = location;
        this.header = header;
        this.headerFile = header.toFile();
        this.tempHeader = header.resolveSibling(header.getFileName()+".temp");
        this.channel = channel;
        this.name = name;
    }

    public static MultiThreadedFileHeap create(Path location, String name, boolean dump) throws IOException, IllegalStateException {

        Files.createDirectories(location);

        Path headerPath = location.resolve(name + ".json");
        if(Files.notExists(headerPath))
            Files.createFile(headerPath);

        FileChannel channel = FileChannel.open(location.resolve(name + ".bin"),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

        var heap = new MultiThreadedFileHeap(location, name, headerPath, channel);

        for(int i = 0; i < numBins; i++)
            heap.freeBlockBin[i] = new ConcurrentLinkedQueue<>();

        if(dump) {
            heap.dumpTruck = new DumpTruck(location, name, 8192);
            heap.enableDumping();
        }

        heap.init();

        return heap;
    }

    private void init() throws IOException, IllegalStateException {

        long start = System.nanoTime();

        if(headerFile.length() == 0) {
            if(dumpTruck.isEnabled()) dumpTruck.dump("Empty header, loaded defaults");
            return;
        }

        Header header;
        try {
            header = mapper.readValue(headerFile, Header.class);
        } catch(DatabindException ex) {
            throw new IllegalStateException("Invalid header, structure mismatch", ex);
        } catch (StreamReadException ex) {
            throw new IllegalStateException("Invalid header, broken json", ex);
        }


        // To avoid runtime problems, I have no idea, this part of code is very uncomfortable but IDC ENOUGH

        if(header.handleCounter < 0) throw new IllegalStateException("Invalid header, handleCounter < 0");
        int handleCounter = header.handleCounter;
        this.handleCounter.set(handleCounter);

        if(header.slackSpace < 0) throw new IllegalStateException("Invalid header, slackSpace < 0");
        slackSpace.set(header.slackSpace);

        if(header.nextOffset < 0) throw new IllegalStateException("Invalid header, nextOffset < 0");
        nextOffset.set(header.nextOffset);

        if((header.allocated.isEmpty() && header.free.isEmpty()) && (handleCounter != 0 || header.nextOffset != 0))
            throw new IllegalStateException("Invalid header, expected allocated/free blocks");

        if(dumpTruck.isEnabled()) dumpTruck.dump("Loaded magic numbers { counter: %d ; slack: %d ; nextOffset: %d }",
                header.handleCounter, slackRate, header.nextOffset);

        dumpTruck.pauseFlushing();

        // READ ALLOCATED-/-USED BLOCKS

        if(dumpTruck.isEnabled()) dumpTruck.dump("Loading (%d) allocated blocks", header.allocated.size());

        for (Header.AllocatedBlock block : header.allocated) {
            if (block.handle < 0 || block.handle >= handleCounter || block.offset < 0 || block.blockSize <= 0 || block.itemSize < 0)
                throw new IllegalStateException("Invalid allocated block { handle: "+block.blockSize+"/"+(handleCounter-1)+"; offset: "+block.offset+"; size: "+block.blockSize+"; item: "+block.itemSize+" }");

            Block realBlock = new Block(block.offset, block.blockSize);
            realBlock.itemSize = block.itemSize;

            usedBlocksByHandle.put(block.handle, realBlock);

            if(dumpTruck.isEnabled()) dumpTruck.dump("Inserted Allocated Block { handle: %d/%d ; offset: %d ; blockSize: %d ; itemSize: %d }",
                    block.handle, (handleCounter-1), block.offset, block.blockSize, block.itemSize);
        }

        // READ FREE BLOCKS

        if(dumpTruck.isEnabled()) dumpTruck.dump("Loading (%d) free blocks", header.free.size());

        for (Header.Block block : header.free) {
            if (block.offset < 0 || block.blockSize <= 0)
                throw new IllegalStateException("Invalid free block { offset: "+block.offset+"; size: "+block.blockSize+" }");

            Block realBlock = new Block(block.offset, block.blockSize);

            freeBlocksByOffset.put(block.offset, realBlock);
            binPush(realBlock);

            if(dumpTruck.isEnabled()) dumpTruck.dump("Inserted Free Block { offset: %d ; blockSize: %d ; bin: %d }",
                    block.offset, block.blockSize, binIndexFor(block.blockSize));
        }

        dumpTruck.resumeFlushing();
        dumpTruck.trimChunk();

        if(dumpTruck.isEnabled()) {
            long duration = System.nanoTime()-start;
            dumpTruck.dump(() -> String.format("Init took (%s ms)", Recorder.asMillis(new StringBuilder(), duration)));
        }

    }

    public Future<Void> saveHeader() {
        // Setup marking dirty
        return ioWorker.submit(() -> {
            saveHeaderNow();
            return null;
        });
    }

    public void saveHeaderNow() {

        snapShotLock.writeLock().lock();
        try {
            long start = System.nanoTime();

            List<Header.AllocatedBlock> allocatedBlocks = new ArrayList<>(usedBlocksByHandle.size());
            for(var entry : usedBlocksByHandle.entrySet())
                allocatedBlocks.add(entry.getValue().snap(entry.getKey()));

            List<Header.Block> freeBlocks = new ArrayList<>(freeBlocksByOffset.size());
            for(var entry : freeBlocksByOffset.entrySet())
                if(entry.getValue().valid) freeBlocks.add(entry.getValue().snap());

            Header header = new Header(handleCounter.get(), nextOffset.get(), slackSpace.get(), allocatedBlocks, freeBlocks);
            try {
                mapper.writerWithDefaultPrettyPrinter().writeValue(tempHeader.toFile(), header);

                // Also renames
                try {
                    Files.move(tempHeader, this.header, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempHeader, this.header, StandardCopyOption.REPLACE_EXISTING);
                }


                if(dumpTruck.isEnabled()) {
                    long duration = System.nanoTime()-start;
                    dumpTruck.dump("Saved header, took (%s ms)", Recorder.asMillis(new StringBuilder(), duration));
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to save header", ex);
            }
        } finally {
            snapShotLock.writeLock().unlock();
        }

    }

    public void shutdown() {

        long start = System.nanoTime();

        ioWorker.shutdown();
        saveHeaderNow();
        if(dumpTruck.isEnabled()) dumpTruck.disable(); // flushes too

    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    // Accessed in allocating, freeing, writing and reading
    private final ConcurrentHashMap<Integer, Block> usedBlocksByHandle = new ConcurrentHashMap<>();

    // Accessed only in allocating and freeing
    private final ConcurrentSkipListMap<Long, Block> freeBlocksByOffset = new ConcurrentSkipListMap<>();

    /* Min power represents the upper bound of smallest bin size range
     * Max power represents the upper bound of largest bin size range
     * Example :
     *   2^13 is 8KB; 13 min power represents a min bin of 4KB to 8KB
     *   2^20 is 1MB; 20 max power represents a max bin of 512KB to 1MB */
    private static final int minAllocationPower = 13, maxAllocationPower = 17;

    /* Is used to decide the minimum allowed leftover spaces after allocation
     * No block smaller than this size can be allocated
     * All items smaller than this size are simply stored with extra space */
    private static final int minAllocSize = (int) Math.pow(2, minAllocationPower-1),
            maxAllocSize = (int) Math.pow(2, maxAllocationPower), // Not actually max allowed
            maxBinIndex = maxAllocationPower-minAllocationPower,
            numBins = maxBinIndex+1,
            minLeftOverSize = minAllocSize; // change this to be adjustable in constructor


    private final ConcurrentLinkedQueue<Block>[] freeBlockBin = new ConcurrentLinkedQueue[numBins];

    private int binIndexFor(long size) {
        if(size < minAllocSize) return 0;

        // Gets a value that as 2^(value) is a number that can occupy this size
        // This value represents the passed number as ceiling power of 2
        int power = 64 - Long.numberOfLeadingZeros(size);

        return Math.clamp(power-minAllocationPower, 0, maxBinIndex);
    }

    private Block pollBin(int binIndex) {

        var bin = freeBlockBin[binIndex];

        if(!bin.isEmpty()) {
            while(true) {
                Block block = bin.poll();
                if(block == null) break;

                /* The idea is, that instead of removing a free block O(n)
                 * The code marks it as invalid and this function removes it performantly */
                if(block.valid) return block;
            }
        }

        int next = binIndex+1;
        return next < numBins ? pollBin(next) : null;

    }

    private void binPush(Block block) {
        var index = binIndexFor(block.blockSize);
        var bin = freeBlockBin[index];
        bin.add(block);
    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //

    private ReentrantReadWriteLock snapShotLock = new ReentrantReadWriteLock();


    /*
     * Safely convert a free block of sufficient size into the state of a used block
     * assuming the used block is then referenced by handle and eventually freed
     * (prone to leaks if not freed) (prone to free block fragmentation)
     *
     * blockAllocatorLock is crucial to allow parallel threads to safely take out a free block
     * freeBlockBin through pollBin is used to select a free block of suitable size without any searching
     */

    public CompletableFuture<Integer> allocateBlock(int size) {

        return CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            snapShotLock.readLock().lock();
            try {
                int occupy = Math.max(size, minAllocSize);
                Block free = null;

                free = pollBin(binIndexFor(occupy));


                if(free != null) {
                    freeBlocksByOffset.remove(free.offset);

                    if(dumpTruck.isEnabled()) dumpTruck.dump("Claimed free block { offset: %d ; blockSize: %d }",
                            free.offset, free.blockSize);

                    // Separate leftover
                    int leftOver = free.blockSize - occupy;

                    if(leftOver < minLeftOverSize) {
                        // Take the whole thing!
                        occupy += leftOver;
                    } else {
                        // Separate the leftover free space
                        Block leftOverBlock = new Block(free.offset + size, leftOver);

                        binPush(leftOverBlock);
                        freeBlocksByOffset.put(leftOverBlock.offset, leftOverBlock);

                        if(dumpTruck.isEnabled()) dumpTruck.dump("Separated extra block space from after allocation { offset: %d ; blockSize: %d }",
                                leftOverBlock.offset, leftOverBlock.blockSize);
                    }

                } else {
                    try {
                        free = newBlock(occupy);
                        if(dumpTruck.isEnabled()) dumpTruck.dump("Created new block for allocation { offset: %d ; blockSize: %d }",
                                free.offset, free.blockSize);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                    // It will have nothing left over
                }

                Block block = new Block(free.offset, occupy);
                int handle = allocateHandle();

                usedBlocksByHandle.put(handle, block);

                if(dumpTruck.isEnabled()) {
                    long duration = System.nanoTime()-start;
                    dumpTruck.dump(() -> String.format("Allocated block { handle: %d ; offset: %d ; blockSize: %d } took (%s ms)",
                            handle, block.offset, block.blockSize, Recorder.asMillis(new StringBuilder(), duration)));
                }

                return handle;
            } finally {
                snapShotLock.readLock().unlock();
            }

        }, ioWorker);

    }

    public CompletableFuture<Void> freeBlock(int handle) {

        Block block = usedBlocksByHandle.get(handle);
        if(block == null) throw new IllegalArgumentException("Invalid handle");

        return CompletableFuture.supplyAsync(() -> {

            long start = System.nanoTime();

            snapShotLock.readLock().lock();
            try {

                if(dumpTruck.isEnabled()) dumpTruck.dump("Freeing block { handle: %d ; offset: %d ; blockSize: %d }", handle, block.offset, block.blockSize);

                long freeOffset = block.offset;
                int freeSize = block.blockSize;

                block.invalidate();
                usedBlocksByHandle.remove(handle);
                freeHandle(handle);

                var next = freeBlocksByOffset.get(block.offset + block.blockSize);
                if(next != null) {
                    next.invalidate(); // Garbage collected by pollBin
                    freeSize += next.blockSize;

                    if(dumpTruck.isEnabled()) dumpTruck.dump("Acquired succeeding free block { offset: %d ; blockSize: %d } for freeing (%d)",
                            next.offset, next.blockSize, handle);
                }

                var entry = freeBlocksByOffset.lowerEntry(block.offset);
                if(entry != null) {
                    var prev = entry.getValue();
                    if(prev != null && prev.offset + prev.blockSize == block.offset) {
                        prev.invalidate();
                        freeSize += prev.blockSize;
                        freeOffset = prev.offset;

                        if(dumpTruck.isEnabled()) dumpTruck.dump("Acquired preceding free block { offset: %d ; blockSize: %d } for freeing (%d)",
                                prev.offset, prev.blockSize, handle);
                    }
                }

                Block free = new Block(freeOffset, freeSize);
                freeBlocksByOffset.put(freeOffset, free);
                binPush(free);


                if(dumpTruck.isEnabled()) {
                    long duration = System.nanoTime()-start;
                    dumpTruck.dump(() -> String.format("Declared free block { offset: %d ; blockSize: %d } took (%s ms)",
                            free.offset, free.blockSize, Recorder.asMillis(new StringBuilder(), duration)));
                }

                return null;

            } finally {
                snapShotLock.readLock().unlock();
            }
        }, ioWorker);

    }



    /* To performantly maintain a locking mechanism for each block, to allow concurrent reads
     * But block any reads during write but the write waits for the last read lock to be freed */

    public CompletableFuture<Void> write(int handle, ByteBuffer src) {

        Block block = usedBlocksByHandle.get(handle);
        if(block == null) throw new IllegalArgumentException("Invalid handle");

        return CompletableFuture.supplyAsync(() -> {
            try {
                int bytes = src.remaining();
                block.writeItem(channel, src);
                if(dumpTruck.isEnabled()) dumpTruck.dump("Finished writing (%d) bytes to block (%d)", bytes, handle);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            return null;
        }, ioWorker);

    }

    public CompletableFuture<Void> read(int handle, ByteBuffer dest) {
        Block block = usedBlocksByHandle.get(handle);
        if(block == null) throw new IllegalArgumentException("Invalid handle");

        return CompletableFuture.supplyAsync(() -> {
            try {
                int marker = dest.position();
                block.readItem(channel, dest);
                if(dumpTruck.isEnabled()) dumpTruck.dump("Finished reading (%d) bytes from block (%d)", dest.limit()-marker, handle);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            return null;
        }, ioWorker);

    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    /*
     * Creates a new block at end of file
     * the function only has one physical reaction, of expanding the file
     *
     * its result is to be handled during allocation for
     *   data manipulation, access referencing and free space separation
     */
    private synchronized Block newBlock(int size) throws IOException {
        if(slackSpace.get() < size)
            expandFile(size); // a simple way that makes sure at least the required free block can be accommodated

        Block block = new Block(nextOffset.get(), size);
        nextOffset.addAndGet(size);
        slackSpace.addAndGet(-size);

        return block;
    }

    private synchronized void expandFile(int extra) throws IOException {
        channel.write(ByteBuffer.wrap(new byte[]{0}), (channel.size() + slackRate + extra)-1);
        slackSpace.addAndGet(slackRate + extra);
    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    public void enableDumping() {
        dumpTruck.enable();
    }

    public void disableDumping() {
        dumpTruck.disable();
    }

    public void flushDump() {
        if(dumpTruck.isEnabled()) dumpTruck.flush();
    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    private final ConcurrentLinkedQueue<Integer> freeHandles = new ConcurrentLinkedQueue<>();
    // Read from and written to header
    // Represents the next usable handle
    private final AtomicInteger handleCounter = new AtomicInteger();

    private int allocateHandle() {
        if(!freeHandles.isEmpty())
            return freeHandles.poll();

        return handleCounter.getAndIncrement();
    }

    private void freeHandle(int handle) {
        freeHandles.add(handle);
    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    private static class Block {

        public boolean valid = true;
        public long offset;
        public int blockSize;
        public int itemSize;

        // Useful to merge this block with a previous free block if there's one
        @Setter public Block previous;

        public Block(long offset, int blockSize) {
            this.offset = offset;
            this.blockSize = blockSize;
        }

        public boolean fits(int size) {
            return size >= 0 && size <= blockSize;
        }

        public synchronized void invalidate() {
            valid = false;
        }

        public synchronized Header.Block snap() {
            return new Header.Block(offset, blockSize);
        }

        public synchronized Header.AllocatedBlock snap(int handle) {
            return new Header.AllocatedBlock(offset, blockSize, handle, itemSize);
        }

        /* Low level write and read util methods
         * These are to be run wrapped into high level async safe code
         */

        protected synchronized void writeItem(FileChannel channel, ByteBuffer src) throws IOException, IllegalStateException {

            if(!valid) throw new IllegalStateException("Tried to write to invalidated block");

            int size = src.remaining();
            if(!fits(size)) throw new IllegalArgumentException("Block cannot fit item ("+size+"/"+blockSize+")");

            long position = offset;
            long goal = offset+size;
            while(position < goal) {
                int written = channel.write(src, position);

                if(written == 0 && src.hasRemaining()) { // Should never happen!
                    itemSize = -1;
                    throw new IllegalStateException("Entered 0 byte writing loop before reaching goal in attempt to insert item into slot");
                }

                position += written;
            }

            itemSize = size;
        }

        protected synchronized void readItem(FileChannel channel, ByteBuffer dest) throws IOException, IllegalStateException {

            if(!valid) throw new IllegalStateException("Tried to write to invalidated block");

            int capacity = dest.remaining();
            if(itemSize > capacity)
                throw new IllegalArgumentException("Block cannot fit item ("+itemSize+"/"+dest.remaining()+")");

            // FileChannel#read - reads up till buffer limit, so that should be item size
            if(itemSize < capacity)
                dest.limit(dest.position()+itemSize);


            long position = offset;
            long goal = position+itemSize;
            while(position < goal) {
                int read = channel.read(dest, position);

                if(read == -1) // Should never happen!!
                    throw new IllegalStateException("Tried to read beyond EOF in attempt to extract item from slot");

                if (read == 0) { // Should never happen!!
                    if(dest.hasRemaining()) // If this happens and buffer also isn't fully written to
                        throw new IllegalStateException("Entered 0 byte reading loop before reaching goal in attempt to extract item from slot");
                }

                position += read;
            }

            dest.flip();
        }

    }


    // ------ // ------ // ------ // ------ // ------ // ------ // ------ //


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    private static class Header {

        final int handleCounter;
        final long nextOffset;
        final int slackSpace;

        final List<AllocatedBlock> allocated;
        final List<Block> free;

        @JsonCreator
        public Header(
                @JsonProperty(value = "handleCounter", required = true) int handleCounter,
                @JsonProperty(value = "nextOffset", required = true) long nextOffset,
                @JsonProperty(value = "slackSpace", required = true) int slackSpace,
                @JsonProperty(value = "allocated", required = true) List<AllocatedBlock> allocated,
                @JsonProperty(value = "free", required = true) List<Block> free
        ) {
            this.handleCounter = handleCounter;
            this.nextOffset = nextOffset;
            this.slackSpace = slackSpace;
            this.allocated = Collections.unmodifiableList(allocated);
            this.free = Collections.unmodifiableList(free);
        }

        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
        private static class Block {
            final long offset;
            final int blockSize;

            @JsonCreator
            Block(
                    @JsonProperty(value = "offset", required = true) long offset,
                    @JsonProperty(value = "blockSize", required = true) int blockSize
            ) {
                this.offset = offset;
                this.blockSize = blockSize;
            }
        }

        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
        private static class AllocatedBlock extends Header.Block {
            final int handle;
            final int itemSize;

            @JsonCreator
            public AllocatedBlock(
                    @JsonProperty(value = "offset", required = true) long offset,
                    @JsonProperty(value = "blockSize", required = true) int blockSize,
                    @JsonProperty(value = "handle", required = true) int handle,
                    @JsonProperty(value = "itemSize", required = true) int itemSize
            ) {
                super(offset, blockSize);
                this.handle = handle;
                this.itemSize = itemSize;
            }
        }


    }


}
