package com.mxp.mdb.backend.parser;

import com.mxp.mdb.backend.parser.statement.*;
import com.mxp.mdb.backend.tbm.Field;
import com.mxp.mdb.common.error.Error;

import java.util.ArrayList;
import java.util.List;

/**
 * <begin statement>
 * begin [isolation level (read committed|repeatable read)]
 * begin isolation level read committed
 * <p>
 * <commit statement>
 * commit
 * <p>
 * <rollback statement>
 * rollback
 * <p>
 * <create statement>
 * create table <table name>
 * <field name> <field type>
 * <field name> <field type>
 * ...
 * <field name> <field type>
 * [(index <field name list>)]
 * create table students
 * id int32,
 * name string,
 * age int32,
 * (index id name)
 * <p>
 * <drop statement>
 * drop table <table name>
 * drop table students
 * <p>
 * <select statement>
 * select (*|<field name list>) from <table name> [<where statement>]
 * select * from student where id = 1
 * select name from student where id > 1 and id < 4
 * select name, age, id from student where id = 12
 * <p>
 * <insert statement>
 * insert into <table name> values <value list>
 * insert into student values 5 "Zhang Yuanjia" 22
 * <p>
 * <delete statement>
 * delete from <table name> <where statement>
 * delete from student where name = "Zhang Yuanjia"
 * <p>
 * <update statement>
 * update <table name> set <field name>=<value> [<where statement>]
 * update student set name = "ZYJ" where id = 5
 * <p>
 * <where statement>
 * where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
 * where age > 10 or age < 3
 * <p>
 * <field name> <table name>
 * [a-zA-Z][a-zA-Z0-9_]*
 * <p>
 * <field type>
 * int32 int64 string
 *
 * <value>
 * .*
 * <p>
 *
 * @author mxp
 * @date 2023/4/18 9:10
 */
public class ParserSql {

    public static Object parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String peek = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception staErr = null;
        try {
            switch (peek) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseRollback(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throwInvalidCommandException();
            }
        } catch (Exception e) {
            staErr = e;
        }

        try {
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                staErr = new RuntimeException("Invalid statement: " + new String(tokenizer.errStat()));
            }
        } catch(Exception e) {
            e.printStackTrace();
            staErr = new RuntimeException("Invalid statement: " + new String(tokenizer.errStat()));
        }

        if (staErr != null) {
            throw staErr;
        }
        return stat;
    }

    /**
     * <begin statement>
     * begin [isolation level (read committed|repeatable read)]
     * begin isolation level read committed
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }

        if (!"isolation".equals(isolation)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();

        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        String peek = tokenizer.peek();
        if ("read".equals(peek)) {
            tokenizer.pop();
            if (!"committed".equals(tokenizer.peek())) {
                throwInvalidCommandException();
            }
            tokenizer.pop();
            if (!"".equals(tokenizer.peek())) {
                throwInvalidCommandException();
            }
        } else if ("repeatable".equals(peek)) {
            tokenizer.pop();
            if (!"read".equals(tokenizer.peek())) {
                throwInvalidCommandException();
            }
            tokenizer.pop();
            if (!"".equals(tokenizer.peek())) {
                throwInvalidCommandException();
            }
            begin.isRepeatableRead = true;
        } else {
            throwInvalidCommandException();
        }
        return begin;
    }

    /**
     * <commit statement>
     * commit
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"".equals(peek)) {
            throwInvalidCommandException();
        }
        return new Commit();
    }

    /**
     * <rollback statement>
     * rollback
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Rollback parseRollback(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"".equals(peek)) {
            throwInvalidCommandException();
        }
        return new Rollback();
    }

    /**
     * <create statement>
     * create table <table name>
     * <field name> <field type> ,
     * <field name> <field type>
     * ...
     * <field name> <field type>
     * [(index <field name list>)]
     *
     * create table students
     * id int32,
     * name string,
     * age int32,
     * (index id name)
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Create parseCreate(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"table".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throwInvalidCommandException();
        }

        List<String> fName = new ArrayList<>();
        List<String> fType = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String s = tokenizer.peek();
            if ("(".equals(s)) {
                break;
            }

            if (!isName(s)) {
                throwInvalidCommandException();
            }
            tokenizer.pop();
            String f = tokenizer.peek();
            if (!isType(f)) {
                throwInvalidCommandException();
            }
            fName.add(s);
            fType.add(f);

            tokenizer.pop();
            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throwTableNoIndexException();
            } else if ("(".equals(next)) {
                break;
            } else {
                throwInvalidCommandException();
            }
        }

        if (fName.isEmpty() || fType.isEmpty()) {
            throwInvalidCommandException();
        }

        Create create = new Create();
        create.tableName = tableName;
        create.fieldName = fName.toArray(new String[0]);
        create.fieldType = fType.toArray(new String[0]);
        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throwInvalidCommandException();
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throwInvalidCommandException();
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[0]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throwInvalidCommandException();
        }
        return create;
    }

    /**
     * <drop statement>
     * drop table <table name>
     *
     * exp:
     * drop table students
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"table".equals(peek)) {
            throwInvalidCommandException();
        }

        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        if(!"".equals(tokenizer.peek())) {
            throwInvalidCommandException();
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    /**
     * <select statement>
     * select (*|<field name list>) from <table name> [<where statement>]
     *
     * exp:
     * select * from student where id = 1
     * select name from student where id > 1 and id < 4
     * select name, age, id from student where id = 12
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Select parseSelect(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        List<String> fields = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            if ("*".equals(peek)) {
                fields = null;
                break;
            }

            if (!isName(peek) || "from".equals(peek)) {
                throwInvalidCommandException();
            }

            fields.add(peek);
            String next = tokenizer.peek();
            if ("".equals(next)) {
                throwInvalidCommandException();
            } else if (",".equals(next)) {
                continue;
            } else if ("from".equals(next)) {
                break;
            }
        }
        tokenizer.pop();
        String s = tokenizer.peek();
        if (!"from".equals(s)) {
            throwTableNoIndexException();
        }
        tokenizer.pop();

        Select select = new Select();
        if (fields != null) {
            select.fields = fields.toArray(new String[0]);
        }

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        select.tableName = tableName;

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            select.where = null;
            return select;
        }

        select.where = parseWhere(tokenizer);
        return select;
    }

    /**
     * <where statement>
     * where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
     *
     * exp:
     * where age > 10 or age < 3
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"where".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        SingleExpression singleExpression = parseSingleExp(tokenizer);
        Where where = new Where();
        where.singleExp1 = singleExpression;
        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }

        if (!isLogicOp(logicOp)) {
            throwInvalidCommandException();
        }

        where.logicOp = logicOp;
        tokenizer.pop();

        where.singleExp2 = parseSingleExp(tokenizer);
        if (!"".equals(tokenizer.peek())) {
            throwInvalidCommandException();
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        String field = tokenizer.peek();
        if(!isName(field)) {
            throwInvalidCommandException();
        }

        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throwInvalidCommandException();
        }

        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    /**
     * <insert statement>
     * insert into <table name> values <value list>
     *
     * exp:
     * insert into student values 5 "Zhang Yuanjia" 22
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"into".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        Insert insert = new Insert();
        insert.tableName = tokenizer.peek();
        tokenizer.pop();
        String s = tokenizer.peek();
        if (!"values".equals(s)) {
            throwInvalidCommandException();
        }
        List<String> values = new ArrayList<>();
        String next;
        while (true) {
            tokenizer.pop();
            if ("".equals(next = tokenizer.peek())) {
                break;
            }
            values.add(next);
        }

        insert.values = values.toArray(new String[0]);
        return insert;
    }

    /**
     * <update statement>
     * update <table name> set <field name>=<value> [<where statement>]
     *
     * exp:
     * update student set name = "ZYJ" where id = 5
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!isName(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();

        Update update = new Update();
        update.tableName = peek;
        peek = tokenizer.peek();

        if (!"set".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        peek = tokenizer.peek();
        if (!"=".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        update.value = tokenizer.peek();
        tokenizer.pop();

        peek = tokenizer.peek();
        if ("".equals(peek)) {
            update.where = null;
            return update;
        }
        update.where = parseWhere(tokenizer);
        return update;
    }

    /**
     * <delete statement>
     * delete from <table name> <where statement>
     *
     * exp:
     * delete from student where name = "mxp"
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    public static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        String peek = tokenizer.peek();
        if (!"from".equals(peek)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();
        Delete delete = new Delete();
        delete.tableName = tokenizer.peek();
        if (!isName(delete.tableName)) {
            throwInvalidCommandException();
        }
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if(!"".equals(tmp)) {
            throwInvalidCommandException();
        }
        return new Show();
    }

    private static boolean isType(String tp) {
        return Field.allowFieldName.contains(tp);
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static void throwInvalidCommandException() throws Exception {
        throw Error.InvalidCommandException;
    }

    private static void throwTableNoIndexException() throws Exception {
        throw Error.TableNoIndexException;
    }
}
