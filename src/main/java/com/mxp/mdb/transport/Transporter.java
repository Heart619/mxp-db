package com.mxp.mdb.transport;

import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Parser;

import java.io.*;
import java.net.Socket;

/**
 * @author mxp
 * @date 2023/4/20 16:16
 */
public class Transporter {

    private Socket socket;
    private BufferedInputStream reader;
    private BufferedOutputStream writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedInputStream(socket.getInputStream());
        this.writer = new BufferedOutputStream(socket.getOutputStream());
    }

    public void send(Package pkg) throws Exception {
        byte[] raw = Encoder.encode(pkg);
        writer.write(ArrayUtil.concat(Parser.intToByte(raw.length), raw));
        writer.flush();
    }

    public Package receive() throws Exception {
        byte[] raw = new byte[Integer.BYTES];
        raw = new byte[Parser.parseInt(raw)];
        return Encoder.decode(raw);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }
}
