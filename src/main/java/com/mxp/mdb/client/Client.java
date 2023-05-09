package com.mxp.mdb.client;

import com.mxp.mdb.transport.Package;
import com.mxp.mdb.transport.Transporter;

/**
 * @author mxp
 * @date 2023/4/20 16:53
 */
public class Client {

    private RoundTripper rt;

    public Client(Transporter transporter) {
        this.rt = new RoundTripper(transporter);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}
