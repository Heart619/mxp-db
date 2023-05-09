package com.mxp.mdb.backend.dm.logger;

import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 *
 * @author mxp
 * @date 2023/4/12 21:33
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 16191;

    private static final int OFFSET_X_CHECKSUM_LEN = 4;

    private static final int OFFSET_SIZE = 0;
    private static final int OFFSET_CHECKSUM = OFFSET_SIZE + 4;
    private static final int OFFSET_DATA = OFFSET_CHECKSUM + 4;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    /**
     * 当前日志指针的位置
     */
    private long position;

    /**
     * 初始化时记录，log操作不更新
     */
    private long fileSize;

    private int xChecksum;

    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum) {
        this(file, fc);
        this.xChecksum = xChecksum;
        position = OFFSET_X_CHECKSUM_LEN;
    }

    private void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (size < OFFSET_X_CHECKSUM_LEN) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;

        checkAndRemoveTail();
    }

    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        byte[] log;
        while ((log = internNext()) != null) {
            xCheck = calCheckSum(xCheck, Arrays.copyOfRange(log, OFFSET_DATA, log.length));
        }

        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            // 截断文件到正常日志的末尾
            truncate(position);
            file.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        rewind();
    }

    /**
     * 计算校验和
     * @param xCheck
     * @param log
     * @return
     */
    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    @Override
    public void log(byte[] data) {
        byte[] raw = wrapLog(data);
        ByteBuffer log = ByteBuffer.wrap(raw);
        lock.lock();
        try {
            fc.position(position);
            fc.write(log);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        position += raw.length;
        updateXCheckSum(data);
    }

    private void updateXCheckSum(byte[] data) {
        this.xChecksum = (int) calCheckSum(this.xChecksum, data);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.intToByte(this.xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] size = Parser.intToByte(data.length);
        byte[] checkSum = Parser.intToByte(calCheckSum(0, data));
        return ArrayUtil.concat(size, checkSum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OFFSET_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 每条正确日志的格式为：
     * [Size] [Checksum] [Data]
     * Size 4字节int 标识Data长度
     * Checksum 4字节int
     *
     * @return
     */
    private byte[] internNext() {
        if (position + OFFSET_DATA > fileSize) {
            return null;
        }
        // [Size]
        ByteBuffer buffer = ByteBuffer.allocate(OFFSET_CHECKSUM);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 数据长度
        int size = Parser.parseInt(buffer.array());
        if (position + size + OFFSET_DATA > fileSize) {
            return null;
        }

        buffer = ByteBuffer.allocate(OFFSET_DATA + size);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // [Size] [Checksum] [Data]
        byte[] log = buffer.array();
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OFFSET_DATA, log.length));
        // [Checksum]
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OFFSET_CHECKSUM, OFFSET_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    @Override
    public void rewind() {
        position = OFFSET_X_CHECKSUM_LEN;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public static Logger create(String path) {
        File file = new File(path + File.separator + LOG_FILE_NAME);
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

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buffer = ByteBuffer.wrap(Parser.intToByte(0));
        try {
            fc.position(0);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path) {
        File file = new File(path + File.separator + LOG_FILE_NAME);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if (!file.canWrite() || !file.canRead()) {
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

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
