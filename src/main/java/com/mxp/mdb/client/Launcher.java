package com.mxp.mdb.client;

import com.mxp.mdb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * @author mxp
 * @date 2023/4/20 16:54
 */
public class Launcher {

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Transporter t = new Transporter(socket);
        Client client = new Client(t);
        Shell shell = new Shell(client);
        shell.run();
    }
}
