package com.mxp.mdb.client;

import com.mxp.mdb.transport.Package;
import com.mxp.mdb.transport.Transporter;

/**
 * @author mxp
 * @date 2023/4/20 16:52
 */
public class RoundTripper {

    private Transporter transporter;

    public RoundTripper(Transporter transporter) {
        this.transporter = transporter;
    }

    public Package roundTrip(Package pkg) throws Exception {
        transporter.send(pkg);
        return transporter.receive();
    }

    public void close() throws Exception {
        transporter.close();
    }
}
