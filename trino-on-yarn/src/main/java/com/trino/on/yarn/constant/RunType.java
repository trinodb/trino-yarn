package com.trino.on.yarn.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RunType {

    YARN_SESSION("yarn-session", "trino-session"),
    YARN_PER("yarn-per", "trino-per-job");

    private String name;
    private String code;
}
