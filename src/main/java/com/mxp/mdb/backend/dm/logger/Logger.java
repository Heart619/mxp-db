package com.mxp.mdb.backend.dm.logger;

/**
 * @author mxp
 * @date 2023/4/12 21:30
 */
public interface Logger {

    String LOG_FILE_NAME = "m-db.log";

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

}
