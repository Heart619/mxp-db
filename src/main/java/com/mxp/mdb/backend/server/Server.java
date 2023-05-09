package com.mxp.mdb.backend.server;

import com.mxp.mdb.backend.tbm.TableManager;
import com.mxp.mdb.backend.utils.Panic;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mxp
 * @date 2023/4/20 15:55
 */
public class Server extends Thread {

    public static void main(String[] args) {
        TableManager tableManager = SafeCheck.before();
        try {
            Server server = new Server(9999, tableManager);
            server.setDaemon(true);
            server.start();
            Scanner sc = new Scanner(System.in);
            while (true) {
                if ("shutdown".equals(sc.nextLine())) {
                    SafeCheck.safeClose(tableManager);
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private final int port;
    TableManager tableManager;

    private static ThreadPoolExecutor executor;

    static {
        executor = new ThreadPoolExecutor(
                8,
                8,
                1L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(200),
                new MyThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public Server(int port, TableManager tableManager) {
        this.port = port;
        this.tableManager = tableManager;
    }

    @Override
    public void run() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (Exception e) {
            Panic.panic(e);
        }

        System.out.println("Server listen to port: " + port);
        try {
            while (true) {
                Socket socket = ss.accept();
                executor.execute(new HandleSocket(socket, tableManager));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private static class MyThreadFactory implements ThreadFactory {
        private AtomicInteger integer = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("thread-" + integer.incrementAndGet());
            return thread;
        }
    }
}
