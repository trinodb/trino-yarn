package com.on.yarn.driver;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, YarnException {
        TrinoSession.getDriverInfo(args[0]);
        System.out.println("Hello world!");
    }
}
