package com.mxp.mdb.backend.utils;

/**
 * @author mxp
 * @date 2023/4/6 20:15
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
