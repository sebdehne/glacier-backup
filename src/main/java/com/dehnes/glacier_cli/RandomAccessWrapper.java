package com.dehnes.glacier_cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomAccessWrapper extends InputStream {
    private final RandomAccessFile r;
    private final AtomicInteger bytesRemaining;
    private int markPos = -1;
    private int markBytesRemaining = -1;

    public RandomAccessWrapper(RandomAccessFile r, int size) {
        this.r = r;
        this.bytesRemaining = new AtomicInteger(size);
    }

    @Override
    public long skip(long n) throws IOException {
        return r.skipBytes((int) n);
    }

    @Override
    public int available() throws IOException {
        return bytesRemaining.get();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            markPos = (int) r.getFilePointer();
            markBytesRemaining = bytesRemaining.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (markPos >= 0) {
            r.seek(markPos);
            bytesRemaining.set(markBytesRemaining);
        }
    }

    @Override
    public int read() throws IOException {
        if (bytesRemaining.get() > 0) {
            bytesRemaining.decrementAndGet();
            return r.read();
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesRemaining.get() > 0) {
            int readLen = Math.min(bytesRemaining.get(), len);
            bytesRemaining.set(bytesRemaining.get() - readLen);
            return r.read(b, off, readLen);
        } else {
            return -1;
        }
    }
}
