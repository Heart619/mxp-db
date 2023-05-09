package com.mxp.mdb.backend.tbm;

import com.mxp.mdb.backend.dm.DataManager;
import com.mxp.mdb.backend.im.BTree;
import com.mxp.mdb.backend.parser.statement.SingleExpression;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.ParseStringRes;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 *
 * @author mxp
 * @date 2023/4/18 16:24
 */
public class Field {

    long uid;
    private Table table;
    String fieldName;
    String fieldType;
    private long index;
    private BTree bt;

    public static Set<String> allowFieldName = new HashSet<>(Arrays.asList(
            "int32",
            "int64",
            "string"
    ));

    public Field(long uid, Table table) {
        this.uid = uid;
        this.table = table;
    }

    public Field(Table table, String fieldName, String fieldType, long index) {
        this.table = table;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    public static Field loadField(Table table, long uid) {
        byte[] raw = null;
        try {
            raw = (((TableManagerImpl) table.tbm).vm).read(TransactionManager.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, table).parseSelf(raw);
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes parseString = Parser.parseString(raw);
        fieldName = parseString.str;
        position += parseString.next;
        parseString = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = parseString.str;
        position += parseString.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, raw.length));
        if (index != 0) {
            try {
                bt = BTree.load(index, ((TableManagerImpl)table.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    public static Field createField(Table table, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field field = new Field(table, fieldName, fieldType, 0);
        if (indexed) {
            DataManager dm = ((TableManagerImpl) table.tbm).dm;
            long index = BTree.create(dm);
            BTree tree = BTree.load(index, dm);
            field.index = index;
            field.bt = tree;
        }

        field.parseSelf(xid);
        return field;
    }

    private void parseSelf(long xid) throws Exception {
        byte[] fieldRaw = Parser.stringToByte(fieldName);;
        byte[] typeNameRaw = Parser.stringToByte(fieldType);
        byte[] indexRaw = Parser.longToByte(index);
        this.uid = ((TableManagerImpl) table.tbm).vm.insert(xid, ArrayUtil.concat(fieldRaw, typeNameRaw, indexRaw));
    }

    /**
     * 是否为索引列
     * @return
     */
    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long k = value2Uid(key);
        bt.insert(k, uid);
    }

    public static void typeCheck(String name) throws Exception {
        if (!allowFieldName.contains(name)) {
            throw Error.InvalidFieldException;
        }
    }

    public Object string2Value(String value) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(value);
            case "int64":
                return Long.parseLong(value);
            case "string":
                return value;
            default:
                return null;
        }
    }

    public byte[] value2Raw(Object o) {
        switch (fieldType) {
            case "int32":
                return Parser.intToByte((int) o);
            case "int64":
                return Parser.longToByte((long) o);
            case "string":
                return Parser.stringToByte((String) o);
            default:
                return null;
        }
    }

    public String printValue(Object o) {
        switch (fieldType) {
            case "int32":
                return String.valueOf((int) o);
            case "int64":
                return String.valueOf((long) o);
            case "string":
                return String.valueOf(o);
            default:
                return null;
        }
    }

    public List<Long> search(long l, long r) throws Exception {
        return bt.searchRange(l, r);
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = Integer.BYTES;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = Long.BYTES;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
            default:
        }
        return res;
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object obj = string2Value(exp.value);
        long uid = value2Uid(obj);
        FieldCalRes calRes = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                calRes.left = 0;
                calRes.right = uid;
                if (calRes.right > 0) {
                    --calRes.right;
                }
                break;
            case ">":
                calRes.right = Long.MAX_VALUE;
                calRes.left = uid + 1;
                break;
            case "=":
                calRes.left = calRes.right = uid;
                break;
            default:
        }
        return calRes;
    }

    public long value2Uid(Object key) {
        String s = String.valueOf(key);
        switch (fieldType) {
            case "int32":
                return Long.parseLong(s);
            case "int64":
                return Integer.parseInt(s);
            case "string":
                return Parser.str2Uid(s);
            default:
                return 0;
        }
    }

    @Override
    public String toString() {
        return fieldName + " " + fieldType;
    }
}
