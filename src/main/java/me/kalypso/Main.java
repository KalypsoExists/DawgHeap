package me.kalypso;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {

        try {
            var heap = MultiThreadedFileHeap.create(new File(System.getProperty("user.dir"), "myHeap").toPath(),
                    "myHeap", true);

            var dummyAlloc = heap.allocateBlock(200);

            AtomicInteger handle = new AtomicInteger();
            var dummyWrite = dummyAlloc.thenAccept((h) -> {
                handle.set(h);
                heap.write(handle.get(), ByteBuffer.wrap(new byte[]{127}));
            });
            var dummyRead = dummyWrite.thenRun(() -> heap.read(handle.get(), ByteBuffer.allocate(1)));
            var dummyFree = dummyRead.thenRun(() -> heap.freeBlock(handle.get()));

            Thread.sleep(1000);

            ByteBuffer item1 = ByteBuffer.allocate(Integer.BYTES * 2)
                    .putInt(1)
                    .putInt(2)
                    .flip();

            ByteBuffer item2 = ByteBuffer.allocate(Integer.BYTES * 4)
                    .putInt(9)
                    .putInt(8)
                    .putInt(7)
                    .putInt(6)
                    .flip();

            var allocate1 = heap.allocateBlock(9000).thenAccept((h) -> heap.write(h, item1));
            var allocate2 = heap.allocateBlock(18000).thenAccept((h) -> heap.write(h, item2));

            Thread.sleep(1000);

            heap.shutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}