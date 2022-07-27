package com.trino.on.yarn.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RunType {

    YARN_SESSION("yarn-session"),
    YARN_PER("yarn-per");

    private String name;
}
