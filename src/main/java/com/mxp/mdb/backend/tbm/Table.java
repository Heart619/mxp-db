package com.mxp.mdb.backend.tbm;

import com.mxp.mdb.backend.parser.statement.*;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.ParseStringRes;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.backend.vm.VersionManager;
import com.mxp.mdb.common.error.Error;

import java.util.*;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 *
 * @author mxp
 * @date 2023/4/19 10:01
 */
public class Table {

    TableManager tbm;
    long uid;
    String tableName;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.tableName = tableName;
        this.nextUid = nextUid;
    }

    public static Table loadTable(TableManagerImpl tableManager, long uid) {
        byte[] raw = null;
        try {
            raw = tableManager.vm.read(TransactionManager.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        Table table = new Table(tableManager, uid);
        return table.parseSelf(raw);
    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table table = new Table(tbm, create.tableName, nextUid);
        Set<String> indexed = new HashSet<>(Arrays.asList(create.index));
        for (int i = 0; i < create.fieldName.length; ++i) {
            String fieldName = create.fieldName[i];
            table.fields.add(Field.createField(table, xid, fieldName, create.fieldType[i], indexed.contains(fieldName)));
        }
        return table.parseSelf(xid);
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        tableName = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    private Table parseSelf(long xid) throws Exception {
        byte[] tableNameRaw = Parser.stringToByte(tableName);
        byte[] nextTableRaw = Parser.longToByte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = ArrayUtil.concat(fieldRaw, Parser.longToByte(field.uid));
        }
        uid = (((TableManagerImpl) tbm).vm).insert(xid, ArrayUtil.concat(tableNameRaw, nextTableRaw, fieldRaw));
        return this;
    }


    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, entry2Raw(entry));
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        Field field;
        for (int i = 0; i < values.length; ++i) {
            field = fields.get(i);
            entry.put(field.fieldName, field.string2Value(values[i]));
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = ArrayUtil.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    public String read(long xid, Select select) throws Exception {
        long[] uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();
        VersionManager vm = ((TableManagerImpl) tbm).vm;
        byte[] raw;
        Map<String, Object> map;
        for (long uid : uids) {
            raw = vm.read(xid, uid);
            if (raw != null) {
                map = raw2Entry(raw);
                sb.append(printEntry(map)).append("\n");
            }
        }
        return sb.toString();
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    private long[] parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single;
        Field field = null;
        if (where == null) {
            for (Field f : fields) {
                if (f.isIndexed()) {
                    field = f;
                    break;
                }
            }

            if (field == null) {
                throw Error.TableNoIndexException;
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field f : fields) {
                if (f.fieldName.equals(where.singleExp1.field)) {
                    if (!f.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    field = f;
                    break;
                }
            }
            if (field == null) {
                throw Error.FieldNotFoundException;
            }

            CalWhereRes res = calWhere(field, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = field.search(l0, r0);
        if (!single) {
            uids.addAll(field.search(l1, r1));
        }
        return ArrayUtil.toArray(uids);
    }

    private CalWhereRes calWhere(Field field, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        FieldCalRes r;
        switch (where.logicOp) {
            case "":
                res.single = true;
                r = field.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = field.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = field.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = field.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = field.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l0 < res.l1) {
                    res.l0 = res.l1;
                }
                if (res.r0 > res.r1) {
                    res.r0 = res.r1;
                }
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private Map<String, Object> raw2Entry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>(fields.size());
        for (Field field : fields) {
            Field.ParseValueRes r = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        Field field;
        for (int i = 0; i < fields.size(); ++i) {
            field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public int update(long xid, Update update) throws Exception {
        long[] uids = parseWhere(update.where);
        Field field = null;
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                field = f;
                break;
            }
        }
        if (field == null) {
            throw Error.FieldNotFoundException;
        }
        VersionManager vm = ((TableManagerImpl) tbm).vm;
        int count = 0;
        for (long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null) {
                continue;
            }

            vm.delete(xid, uid);
            Map<String, Object> entry = raw2Entry(raw);
            entry.put(update.fieldName, update.value);
            raw = entry2Raw(entry);
            long uuid = vm.insert(xid, raw);
            ++count;

            for (Field f : fields) {
                if (f.isIndexed()) {
                    f.insert(entry.get(f.fieldName), uuid);
                }
            }
        }
        return count;
    }

    public int delete(long xid, Delete delete) throws Exception {
        long[] uids = parseWhere(delete.where);
        int count = 0;
        VersionManager vm = ((TableManagerImpl) tbm).vm;
        for (long uid : uids) {
            if (vm.delete(xid, uid)) {
                ++count;
            }
        }
        return count;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(tableName).append(": \n");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("\n}");
            } else {
                sb.append(",\n");
            }
        }
        return sb.toString();
    }
}
