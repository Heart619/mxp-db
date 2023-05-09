package com.mxp.mdb.backend.common;

/**
 * @author mxp
 * @date 2023/4/13 19:40
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
