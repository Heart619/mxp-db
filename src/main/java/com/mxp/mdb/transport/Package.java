package com.mxp.mdb.transport;

/**
 * @author mxp
 * @date 2023/4/20 16:07
 */
public class Package {

    private byte[] data;
    private Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
