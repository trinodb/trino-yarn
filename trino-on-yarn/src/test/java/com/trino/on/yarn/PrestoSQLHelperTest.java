package com.trino.on.yarn;

import com.trino.on.yarn.util.PrestoSQLHelper;
import org.junit.Test;

import java.util.List;

public class PrestoSQLHelperTest {
    private static final String sql = "INSERT INTO adc.fsfd with recursive t as (select a,b,v from a.x)  select a,b,v from t;"
            + "insert into bigdata.user.ss select * from users a left outer join address b on a.address_id = b.id";

    @Test
    public void t1() {
        List<String> statementData = PrestoSQLHelper.getStatementData(sql);
        System.out.println(statementData);
    }
}
