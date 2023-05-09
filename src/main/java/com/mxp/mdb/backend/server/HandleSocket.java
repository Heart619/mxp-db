package com.mxp.mdb.backend.server;

import com.mxp.mdb.backend.tbm.TableManager;
import com.mxp.mdb.transport.Package;
import com.mxp.mdb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author mxp
 * @date 2023/4/20 15:59
 */
public class HandleSocket implements Runnable {

    private Socket socket;
    private TableManager tableManager;

    public HandleSocket(Socket socket, TableManager tableManager) {
        this.socket = socket;
        this.tableManager = tableManager;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());
        Transporter transporter;
        try {
            transporter = new Transporter(socket);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }

        Executor exe = new Executor(tableManager);
        Package pkg;
        byte[] sql, result;
        Exception e;
        while (true) {
            try {
                 pkg = transporter.receive();
            } catch (Exception e1) {
                break;
            }

            sql = pkg.getData();
            result = null;
            e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }

            pkg = new Package(result, e);
            try {
                transporter.send(pkg);
            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }

        exe.close();
        try {
            transporter.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}
