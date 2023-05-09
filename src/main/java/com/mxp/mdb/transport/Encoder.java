package com.mxp.mdb.transport;

import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * [size][Flag][data]
 * flag 为 0，表示发送的是数据，那么 data 即为这份数据本身
 * flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息
 *
 * @author mxp
 * @date 2023/4/20 16:07
 */
public class Encoder {

    public static byte[] encode(Package pkg) {
        byte[] raw;
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            raw = ArrayUtil.concat(new byte[]{1}, msg.getBytes(StandardCharsets.UTF_8));
        } else {
            raw = ArrayUtil.concat(new byte[]{0}, pkg.getData());
        }
        return raw;
    }

    public static Package decode(byte[] data) throws Exception {
        byte b;
        if (data.length < 1 || ((b = data[0]) != 1 && b != 0)) {
            throw Error.InvalidPkgDataException;
        }
        byte[] raw = Arrays.copyOfRange(data, 1, data.length);
        if (b == 0) {
            return new Package(raw, null);
        }
        return new Package(null, new RuntimeException(new String(raw)));
    }
}
