package com.trino.on.yarn.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import lombok.val;

import java.util.List;
import java.util.Set;

public class PrestoSQLHelper {
    private static final SqlParser SQL_PARSER = new SqlParser();

    public static List<String> getStatementData(String sqls) {
        Set<String> catalogs = CollUtil.newHashSet();
        ParsingOptions parsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);
        if (StrUtil.isBlank(sqls)) return CollUtil.newArrayList(catalogs);
        for (String sql : StrUtil.split(sqls, ";")) {
            Statement statement = SQL_PARSER.createStatement(sql, parsingOptions);
            if (statement instanceof Insert) {
                Insert insert = (Insert) statement;
                addCatalog(insert.getTarget().toString(), catalogs);
            }
            maxDepthLeaf(statement.getChildren(), catalogs);
        }
        return CollUtil.newArrayList(catalogs);
    }

    private static void maxDepthLeaf(List<? extends Node> treeList, Set<String> catalogs) {
        if (CollUtil.isNotEmpty(treeList)) {
            for (Node node : treeList) {
                List<? extends Node> children = node.getChildren();
                if (node instanceof QuerySpecification) {
                    if (((QuerySpecification) node).getFrom().isPresent()) {
                        val from = ((QuerySpecification) node).getFrom().get();
                        if (from instanceof Table) {
                            addCatalog(((Table) from).getName().toString(), catalogs);
                        }
                    }
                }

                if (node instanceof Table) {
                    addCatalog(((Table) node).getName().toString(), catalogs);
                }

                if (CollUtil.isNotEmpty(children)) {
                    maxDepthLeaf(children, catalogs);
                }
            }
        }
    }

    private static void addCatalog(String connectors, Set<String> catalogs) {
        if (StrUtil.isBlank(connectors)) return;
        String[] split = connectors.split("\\.");
        int length = split.length;
        if (length < 2 || length > 3) return;
        if (length == 2) {
            catalogs.add("hive");
        } else {
            catalogs.add(split[0]);
        }

    }

}
