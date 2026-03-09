package me.kalypso;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class DumpTruck {

    private ExecutorService dumpTruck;
    private int chunkSize;
    private StringBuilder chunk;

    private volatile boolean enabled, flushing = true;
    private Path location, file;
    private final String name;

    public DumpTruck(Path location, String name, int chunkSize) {
        this.chunkSize = chunkSize;
        this.location = location;
        this.file = location.resolve(name+".dump");
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void enable() {
        if(enabled) return;

        /* If there is already a dump file created/present, then separate that file
         * and empty it (delete it)
         */
        StringBuilder sb = new StringBuilder();
        if(Files.exists(file)) {
            Path saveFile; int counter = 1;

            try {
                while(true) {
                    sb.append(name).append(" (").append(counter).append(")").append(".dump");
                    saveFile = location.resolve(sb.toString());
                    if(!Files.exists(saveFile)) {
                        try {
                            Files.move(file, saveFile, StandardCopyOption.REPLACE_EXISTING,
                                    StandardCopyOption.ATOMIC_MOVE);
                        } catch (AtomicMoveNotSupportedException e) {
                            Files.move(file, saveFile, StandardCopyOption.REPLACE_EXISTING);
                        }

                        break;
                    }

                    sb.setLength(0);
                    counter++;
                }

                Files.deleteIfExists(file); // Kind of unnecessary since file is moved, not copied, but ill keep it!

            } catch (IOException ex) {
                throw new IllegalStateException("Failed to separate old dump file", ex);
            }
        }


        sb.setLength(0);
        chunk = sb; // REUSE RECYCLE AND REDUCE!!!
        dumpTruck = Executors.newSingleThreadExecutor();

        enabled = true;
    }

    public void disable() {
        if(!enabled) return;

        enabled = false;

        dumpTruck.execute(() -> {
            dumpTruck.shutdown();

            flushDumpNow();
            dumpTruck = null;
            chunk = null;
        });
    }

    public void flush() {
        if(!flushing) return;
        dumpTruck.execute(this::flushDumpNow);
    }

    private void flushDumpNow() {
        if(!flushing) return;
        try {
            Files.writeString(file, chunk.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            chunk.setLength(0);
        } catch (Exception ex) {
            enabled = false;
            ex.printStackTrace();
        }
    }

    private void tryFlushingHere() {
        if(flushing && chunk.length() >= chunkSize)
            flushDumpNow();
    }

    private void tryFlushing() {
        if(flushing && chunk.length() >= chunkSize)
            flush();
    }


    // Useful for logging multiple logs at once without massive flushing

    public void pauseFlushing() {
        flushing = false;
    }

    public void resumeFlushing() {
        if(flushing) return;

        flushing = true;
        tryFlushing();
    }

    // Useful alongside pausing and unpausing flushing
    public void trimChunk() {
        if(enabled) {
            dumpTruck.execute(() -> {
                tryFlushingHere();
                chunk.trimToSize();
            });
        }
    }




    public void dump(String format, Object... parts) {
        if(enabled) {
            dumpTruck.execute(() -> {
                chunk.append(String.format(format, parts)).append("\n");
                tryFlushingHere();
            });
        }
    }

    public void dump(String message) {
        if(enabled) {
            dumpTruck.execute(() -> {
                chunk.append(message).append("\n");
                tryFlushingHere();
            });
        }
    }

    public void dump(Supplier<String> supplier) {
        if(enabled) {
            dumpTruck.execute(() -> {
                chunk.append(supplier.get()).append("\n");
                tryFlushingHere();
            });
        }
    }

}
