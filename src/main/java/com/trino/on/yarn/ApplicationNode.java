package com.trino.on.yarn;

import cn.hutool.core.thread.ThreadUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ApplicationNode {

    protected static final Log LOG = LogFactory.getLog(ApplicationNode.class);

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            ThreadUtil.sleep(1000);
            LOG.info("Hello World!");
        }
    }
}
