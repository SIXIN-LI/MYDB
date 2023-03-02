package DM.PageCache;

import DM.AbstractCache;
import DM.Page.Page;
import DM.Page.PageImpl;
import common.Errors;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import utils.Panic;

/**
 * This cache will use reference counting framework by extending from AbstractCache
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    public static final int PAGE_SIZE = 8192;
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private ReentrantLock fileLock;
    private RandomAccessFile file;
    private FileChannel fc;
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file,  FileChannel fc, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(Errors.MemTooSmallException);
        }
        // number of bytes
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.fileLock = new ReentrantLock();
        this.file = file;
        this.fc = fc;
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    /**
     * Create a new page and store the given data into it
     * Increment the number of pages
     * Write this page into .db file
     *
     * @param initData
     * @return page number
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pgno, initData, this);
        flush(page);
        return pgno;
    }

    /**
     * Read from file based on offset
     *
     * @param pgno
     * @return
     * @throws Exception
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    /**
     * Retrieve the data from source(file system)
     * and wrap data into Page
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.getPageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }


    /**
     * Only when the page is dirty do we write back to the source
     *
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    /**
     * Change the length of the file
     * TODO: what if the file size is smaller than new size
     *
     * @param maxPgno
     */
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = getPageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.get();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    @Override
    public void close() {
        close();
    }

    private static long getPageOffset(int pgno) {
        // Page number starts from 1
        return (pgno-1) * PAGE_SIZE;
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = getPageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
