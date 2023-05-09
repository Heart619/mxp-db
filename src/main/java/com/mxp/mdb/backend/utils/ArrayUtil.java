package com.mxp.mdb.backend.utils;

import java.util.List;

/**
 * @author mxp
 * @date 2023/4/13 19:48
 */
public class ArrayUtil {

    public static byte[] concat(byte[]... raws) {
        int len = 0;
        for (int i = 0; i < raws.length; ++i) {
            len += raws[i].length;
        }

        byte[] res = new byte[len];
        int pos = 0;
        for (int i = 0; i < raws.length; ++i) {
            System.arraycopy(raws[i], 0, res, pos, raws[i].length);
            pos += raws[i].length;
        }
        return res;
    }

    public static long[] toArray(List<Long> list) {
        long[] res = new long[list.size()];
        for (int i = 0; i < res.length; ++i) {
            res[i] = list.get(i);
        }
        return res;
    }
}
