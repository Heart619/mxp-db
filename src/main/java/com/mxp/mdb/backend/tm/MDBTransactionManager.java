package com.mxp.mdb.backend.tm;

import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在 M-DB 中，每一个事务都有一个 XID，这个 ID 唯一标识了这个事务。事务的 XID 从 1 开始标号，并自增，不可重复。
 * 并特殊规定 XID 0 是一个超级事务（Super Transaction）。
 * 当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。XID 为 0 的事务的状态永远是 committed。
 * <p>
 * TransactionManager 维护了一个 XID 格式的文件，用来记录各个事务的状态。M-DB 中，每个事务都有下面的三种状态：
 * 1、active，正在进行，尚未结束
 * 2、committed，已提交
 * 3、rollback，回滚
 * XID 文件给每个事务分配了一个字节的空间，用来保存其状态。
 * 同时，在 XID 文件的头部，还保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数。
 * 于是，事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID） 的状态不需要记录。
 *
 * @author mxp
 * @date 2023/4/6 19:32
 */
public class MDBTransactionManager implements TransactionManager {

    /**
     * XID文件头长度
     */
    private static final int LEN_XID_HEADER_LENGTH = 8;
    /**
     * 每个事务的占用长度
     */
    private static final int XID_FIELD_SIZE = 1;
    /**
     * 事务的三种状态：<br>
     * 1、active-活跃-0 <br>
     * 2、committed-已提交-1 <br>
     * 3、rollback-回滚-2
     */
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ROLLBACK = 2;

    private final RandomAccessFile file;
    private final FileChannel fc;
    private long xidCounter;
    private final Lock counterLock;

    public MDBTransactionManager(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXidCounter();
    }

    /**
     * XID文件名称
     */
    public static final String XID_NAME = "mdb.xid";

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXidCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 更新xid事务的状态为status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXidCounter() {
        ++xidCounter;
        ByteBuffer buf = ByteBuffer.wrap(Parser.longToByte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        counterLock.lock();
        try {
            updateXID(xid, FIELD_TRAN_COMMITTED);
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void rollback(long xid) {
        counterLock.lock();
        try {
            updateXID(xid, FIELD_TRAN_ROLLBACK);
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public boolean isActive(long xid) {
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isRollback(long xid) {
        return checkXID(xid, FIELD_TRAN_ROLLBACK);
    }

    @Override
    public void close() throws IOException {
        file.close();
        fc.close();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXidCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 检测XID事务是否处于status状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long xidPosition = getXidPosition(xid);
        ByteBuffer buffer = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.position(xidPosition);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return status == buffer.array()[0];
    }

    /**
     * 创建xid文件，并返回MDBTransactionManager对象
     * @param path
     * @return
     */
    public static MDBTransactionManager create(String path) {
        MDBTransactionManager transactionManager = null;
        try {
            File file = new File(path + File.separator + XID_NAME);
            if (!file.exists()) {
                file.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            ByteBuffer buf = ByteBuffer.wrap(new byte[MDBTransactionManager.LEN_XID_HEADER_LENGTH]);
            FileChannel fc = raf.getChannel();
            fc.write(buf);
            transactionManager = new MDBTransactionManager(raf, fc);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return transactionManager;
    }

    /**
     * 从已有xid文件创建MDBTransactionManager对象
     * @param path
     * @return
     */
    public static MDBTransactionManager open(String path) {
        MDBTransactionManager transactionManager = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(path + File.separator + XID_NAME, "rw");
            FileChannel fc = raf.getChannel();
            transactionManager = new MDBTransactionManager(raf, fc);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return transactionManager;
    }
}
