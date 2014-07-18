package com.github.lemniscate.tomcat.session.util;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;

@Slf4j
public class SessionTrackerValve extends ValveBase {

    @Setter
    private Manager manager;

    @Override
    public void invoke(Request request, Response response) throws IOException,
            ServletException {
        try {
            getNext().invoke(request, response);
        } finally {
            storeSession(request, response);
        }
    }

    private void storeSession(Request request, Response response)
            throws IOException {
        final Session session = request.getSessionInternal(false);

        if (session == null) {
            return;
        }

        if (session.isValid()) {
            log.trace(String.format("Request with session completed, saving session %s", session.getId()));
            if(session.getSession() == null){
                log.trace("No HTTP Session present, Not saving " + session.getId());
            }else{
                log.trace("HTTP Session present, saving " + session.getId());
                manager.add(session);
            }
        } else {
            log.trace("HTTP Session has been invalidated, removing :" + session.getId());
            manager.remove(session);
        }
    }
}

