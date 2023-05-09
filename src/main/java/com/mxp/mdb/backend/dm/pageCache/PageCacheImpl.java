package com.mxp.mdb.backend.dm.pageCache;

import com.mxp.mdb.backend.common.AbstractCache;
import com.mxp.mdb.backend.dm.page.Page;
import com.mxp.mdb.backend.dm.page.PageImpl;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.common.error.Error;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/12 18:31
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    public static final String DB_FILE_NAME = "m-db.db";
    private static final int MEM_MIN_LIM = 10;

    private FileChannel fc;
    private RandomAccessFile raf;
    private Lock lock;
    private AtomicInteger pageNumber;

    public PageCacheImpl(FileChannel fc, RandomAccessFile raf, int capacity) {
        super(capacity);
        if (capacity < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.fc = fc;
        this.raf = raf;
        lock = new ReentrantLock();
        pageNumber = new AtomicInteger((int) (length) / PAGE_SIZE);
    }

    /**
     * 获取数据页的相对偏移
     * @param no
     * @return
     */
    public long pageOffset(long no) {
        return (no - 1) * PAGE_SIZE;
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     * @param pageNumber
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long pageNumber) throws Exception {
        long offset = pageOffset(pageNumber);
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        return new PageImpl((int) pageNumber, buffer.array(), this);
    }

    /**
     * 根据是否为脏页决定是否重新刷盘
     * @param obj
     */
    @Override
    protected void releaseForCache(Page obj) {
        if (obj.isDirty()) {
            flush(obj);
            obj.setDirty(false);
        }
    }

    /**
     * 将数据刷入磁盘
     * @param obj
     */
    private void flush(Page obj) {
        long offset = pageOffset(obj.getPageNumber());
        lock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(obj.getData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 创建新数据页
     * @param initData
     * @return
     */
    @Override
    public int newPage(byte[] initData) {
        int no = pageNumber.incrementAndGet();
        Page pg = new PageImpl(no, initData, null);
        flush(pg);
        return no;
    }

    /**
     * 获取数据页
     * 缓存 - 有 -返回
     *     - 无 -磁盘读取，存入缓存
     * @param pageNo
     * @return
     * @throws Exception
     */
    @Override
    public Page getPage(int pageNo) throws Exception {
        return get(pageNo);
    }

    /**
     * 将数据页从缓存中移除
     * @param page
     */
    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumber.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumber.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    public static PageCache create(String path, long memory) {
        File file = new File(path + File.separator + DB_FILE_NAME);
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    Panic.panic(Error.FileNotExistsException);
                }
            } catch (IOException e) {
                Panic.panic(e);
            }
        }

        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(fc, raf, (int) memory / PAGE_SIZE);
    }

    public static PageCache open(String path, long memory) {
        File file = new File(path + File.separator + DB_FILE_NAME);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(fc, raf, (int) memory / PAGE_SIZE);
    }
}
