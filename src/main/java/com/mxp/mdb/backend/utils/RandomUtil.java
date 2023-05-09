package com.mxp.mdb.backend.utils;

import java.util.Random;

/**
 * @author mxp
 * @date 2023/4/12 20:28
 */
public class RandomUtil {

    public static byte[] randomBytes(int length) {
        Random r = new Random();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
