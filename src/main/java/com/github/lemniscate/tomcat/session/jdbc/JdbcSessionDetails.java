package com.github.lemniscate.tomcat.session.jdbc;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class JdbcSessionDetails {
    private String id;
    private String appName;
    private long lastAccess;
    private int maxInactive;
    private byte[] sessionData;
    private boolean valid;
}
