package com.trino.on.yarn.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RunType {

    YARN_SESSION("yarn-session", "trino-per-job"),
    YARN_PER("yarn-per", "trino-session");

    private String name;
    private String code;
}
