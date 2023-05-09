package com.mxp.mdb.backend.tbm;

import com.mxp.mdb.backend.dm.DataManager;
import com.mxp.mdb.backend.parser.statement.*;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.backend.vm.VersionManager;
import com.mxp.mdb.backend.vm.VersionManagerImpl;
import com.mxp.mdb.backend.vm.Visibility;
import com.mxp.mdb.common.error.Error;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/18 16:29
 */
public class TableManagerImpl implements TableManager {

    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        Table table;
        while (uid != 0) {
            table = Table.loadTable(this, uid);
            uid = table.nextUid;
            tableCache.put(table.tableName, table);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.longToByte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        long xid = vm.begin(begin.isRepeatableRead ? Visibility.REPEATABLE_READ : Visibility.READ_COMMITTED);
        BeginRes res = new BeginRes();
        res.xid = xid;
        res.result = "begin".getBytes(StandardCharsets.UTF_8);
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "rollback".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t != null) {
                for (Table tb : t) {
                    sb.append(tb.toString()).append("\n");
                }
            }
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }

            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            List<Table> list;
            if ((list = xidTableCache.get(xid)) == null) {
                xidTableCache.put(xid, (list = new ArrayList<>()));
            }
            list.add(table);
            return ("create " + create.tableName).getBytes(StandardCharsets.UTF_8);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        Table table;
        lock.lock();
        try {
            table = tableCache.get(insert.tableName);
        } finally {
            lock.unlock();
        }
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        Table table;
        lock.lock();
        try {
            table = tableCache.get(select.tableName);
        } finally {
            lock.unlock();
        }
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, select).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        Table table;
        lock.lock();
        try {
            table = tableCache.get(update.tableName);
        } finally {
            lock.unlock();
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        Table table;
        lock.lock();
        try {
            table = tableCache.get(delete.tableName);
        } finally {
            lock.unlock();
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes(StandardCharsets.UTF_8);
    }

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.longToByte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }

    public void close() {
        dm.closeDataManager();

    }
}
