package com.github.lemniscate.tomcat.session.jdbc;

import com.github.lemniscate.tomcat.session.util.SessionSerializer;
import com.github.lemniscate.tomcat.session.util.SessionTrackerValve;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.*;
import org.apache.catalina.session.StandardSession;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.beans.PropertyChangeListener;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class JdbcSessionManager implements Manager, Lifecycle {

    private final RowMapper<JdbcSessionDetails> rowMapper = new BeanPropertyRowMapper<JdbcSessionDetails>(JdbcSessionDetails.class);
    private final ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();

    private SessionSerializer serializer = new SessionSerializer();

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JdbcSessionTableDetails tableDetails;

    private LifecycleState state = LifecycleState.NEW;

    private int maxInactiveInterval = 60 * 30; // 30 minutes
    private int maxActive = 1000000;

    private SessionTrackerValve trackerValve;
    private Container container;
    private Context context;

    @Getter
    @RequiredArgsConstructor
    public static class JdbcSessionTableDetails {
        private final String table, idCol, dataCol, lastAccessCol;
    }


    public JdbcSessionManager(NamedParameterJdbcTemplate jdbcTemplate, JdbcSessionTableDetails tableDetails) {
        this.tableDetails = tableDetails;
        this.jdbcTemplate = jdbcTemplate;
    }


    // ****** Life-Cycle Methods ***************************

    @Override
    public void start() throws LifecycleException {
        boolean found = false;
        // find the valve and set this manager on it
        for (Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof SessionTrackerValve) {
                trackerValve = (SessionTrackerValve) valve;
                trackerValve.setManager(this);
                log.info("Attached to " + trackerValve.getClass().getSimpleName() );
                found = true;
                break;
            }
        }
        if( !found ){
            log.warn(String.format("Did not find %s valve to attach to", SessionTrackerValve.class.getSimpleName()));
        }

    }

    @Override
    public void stop() throws LifecycleException {
        state = LifecycleState.STOPPING;

        state = LifecycleState.STOPPED;
    }

    @Override
    public Session createEmptySession() {
        JdbcSession session = new JdbcSession(this);
        session.setId(UUID.randomUUID().toString());
        session.setMaxInactiveInterval(maxInactiveInterval);
        session.setValid(true);
        session.setCreationTime(System.currentTimeMillis());
        session.setNew(true);
        currentSession.set(session);
        log.info("Created new empty session " + session.getIdInternal());
        return session;
    }

    /**
     * Periodically called by container to clear out old sessions.
     */
    @Override
    public void backgroundProcess() {
        long olderThan = System.currentTimeMillis() - (getMaxInactiveInterval() * 1000);
        String sql = String.format("delete from %s where %s < :olderThan", tableDetails.getTable(), tableDetails.getLastAccessCol());
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("olderThan", olderThan);
        int removed = jdbcTemplate.update(sql, params);
        log.info("Removing {} sessions older than {}", removed, olderThan);
    }

    @Override
    public void add(Session session) {
        try {
            save(session);
        } catch (IOException ex) {
            log.warn("Error adding new session", ex);
        }
    }

    // ****** Simple Getters and Setters ****************************************

    @Override
    public Container getContainer() {
        return container;
    }

    @Override
    public void setContainer(Container container) {
        this.container = container;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void setContext(Context context) {
        this.context = context;
    }

    @Override
    public boolean getDistributable() {
        return false;
    }

    @Override
    public void setDistributable(boolean b) {

    }

    @Override
    public int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    @Override
    public void setMaxInactiveInterval(int i) {
        maxInactiveInterval = i;
    }

    @Override
    public int getSessionIdLength() {
        return 37;
    }

    @Override
    public void setSessionIdLength(int i) {

    }

    @Override
    public int getMaxActive() {
        return maxActive;
    }

    @Override
    public void setMaxActive(int i) {
        this.maxActive = i;
    }

    @Override
    public int getActiveSessions() {
        return 1000000;
    }

    public int getRejectedSessions() {
        return 0;
    }

    @Override
    public int getSessionMaxAliveTime() {
        return maxInactiveInterval;
    }

    @Override
    public void setSessionMaxAliveTime(int i) {

    }

    @Override
    public int getSessionAverageAliveTime() {
        return 0;
    }

    @Override
    public void load() throws ClassNotFoundException, IOException {
    }

    @Override
    public void unload() throws IOException {
    }


    public void addLifecycleListener(LifecycleListener lifecycleListener) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public LifecycleListener[] findLifecycleListeners() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void addPropertyChangeListener(
            PropertyChangeListener propertyChangeListener) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void changeSessionId(Session session) {
        session.setId(UUID.randomUUID().toString());
    }

    @Override
    public void changeSessionId(Session session, String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public org.apache.catalina.Session createSession(String sessionId) {
        StandardSession session = (JdbcSession) createEmptySession();

        log.info("Created session with id " + session.getIdInternal() + " ( " + sessionId + ")");
        if (sessionId != null) {
            session.setId(sessionId);
        }

        return session;
    }

    @Override
    public Session[] findSessions() {
        try {
            List<Session> sessions = new ArrayList<Session>();
            for (String sessionId : keys()) {
                sessions.add(loadSession(sessionId));
            }
            return sessions.toArray(new Session[sessions.size()]);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Session findSession(String id) throws IOException {
        return loadSession(id);
    }

    private String[] keys() throws IOException {
        String sql = String.format("select %s from %s", tableDetails.getIdCol(), tableDetails.getTable());
        List<String> ids = jdbcTemplate.queryForList(sql, Collections.<String, Object> emptyMap(), String.class);
        return ids.toArray(new String[ids.size()]);
    }

    private Session loadSession(String id) throws IOException {

        if (id == null || id.length() == 0) {
            return createEmptySession();
        }

        StandardSession session = currentSession.get();

        if (session != null) {
            if (id.equals(session.getId())) {
                return session;
            } else {
                currentSession.remove();
            }
        }
        try {
            log.info("Loading session " + id);

            JdbcSessionDetails dbSession = lookupSession(id);

            if (dbSession == null) {
                log.info( "Could not find session " + id );
                StandardSession ret = (JdbcSession) createEmptySession();
                ret.setId(id);
                currentSession.set(ret);
                return ret;
            }

            byte[] data = dbSession.getSessionData();

            session = (JdbcSession) createEmptySession();
            session.setId(id);
            session.setManager(this);
            serializer.deserialize(data, session);

            session.setMaxInactiveInterval(-1);
            session.access();
            session.setValid(true);
            session.setNew(false);


//                log.info("Session Contents [" + session.getId() + "]:");
//                for (Object name : Collections.list(session.getAttributeNames())) {
//                    log.info("  " + name);
//                }


            log.info("Loaded session id " + id);
            currentSession.set(session);
            return session;
        } catch (IOException e) {
            log.warn(e.getMessage());
            throw e;
        } catch (ClassNotFoundException ex) {
            log.warn("Unable to deserialize session ", ex);
            throw new IOException("Unable to deserialize into session", ex);
        }
    }

private JdbcSessionDetails lookupSession(String id) {
    String sql = String.format("select * from %s where %s = :id", tableDetails.getTable(), tableDetails.getIdCol());
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", id);
    List<JdbcSessionDetails> dbSessions = jdbcTemplate.query(sql, params, rowMapper);
    return dbSessions.isEmpty() ? null : dbSessions.get(0);
}

private void save(Session session) throws IOException {
        try {
            log.info("Saving session " + session);

            StandardSession standardsession = (JdbcSession) session;


//                log.info("Session Contents [" + session.getId() + "]:");
//                for (Object name : Collections.list(standardsession.getAttributeNames())) {
//                    log.info("  " + name);
//                }


            final String id = standardsession.getId();
            final byte[] obs = serializer.serialize(standardsession);
            final int size = obs.length;

            final ByteArrayInputStream bis = new ByteArrayInputStream(obs, 0, size);
            final InputStream in = new BufferedInputStream(bis, size);

            JdbcSessionDetails existing = lookupSession(id);
            String sql;


            if( existing == null ){
                sql = String.format("insert into %s (%s, %s, %s) values (?, ?, ?) ",
                    tableDetails.getTable(), tableDetails.getLastAccessCol(), tableDetails.getDataCol(), tableDetails.getIdCol() );
                log.info("Creating session with id " + session.getIdInternal());
            }else{
                sql = String.format("update %s set %s = ?, %s = ? where %s = ? ",
                        tableDetails.getTable(), tableDetails.getLastAccessCol(), tableDetails.getDataCol(), tableDetails.getIdCol());
                log.info("Updating session with id " + session.getIdInternal());
            }

            final String theSql = sql;

            int modified = jdbcTemplate.getJdbcOperations().execute(new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
                    PreparedStatement stmt = conn.prepareStatement(theSql);
                    stmt.setLong(1, System.currentTimeMillis() + 1000 * 60 * 60 * 24);
                    stmt.setBinaryStream(2, in, size);
                    stmt.setString(3, id);
                    return stmt;
                }
            }, new PreparedStatementCallback<Integer>() {
                @Override
                public Integer doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    return ps.executeUpdate();
                }
            });

            log.info("Processed {} records", modified);

            try{
                in.close();
                bis.close();
            }catch(IOException e){
                log.warn("Failed while closing streams", e);
            }
        } catch (IOException e) {
            log.warn("Failed saving session", e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            currentSession.remove();
            log.info("Session removed from ThreadLocal :"
                    + session.getIdInternal());
        }
    }

    @Override
    public void remove(Session session) {
        this.remove(session, true);
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    public void init() throws LifecycleException {}

    @Override
    public void destroy() throws LifecycleException {}

    @Override
    public LifecycleState getState() {
        return state;
    }

    @Override
    public String getStateName() {
        return state.toString();
    }

    @Override
    public long getSessionCounter() {
        return 10000000;
    }

    @Override
    public void setSessionCounter(long sessionCounter) {
    }

    @Override
    public long getExpiredSessions() {
        return 0;
    }

    @Override
    public void setExpiredSessions(long expiredSessions) {
    }

    @Override
    public int getSessionCreateRate() {
        return 0;
    }

    @Override
    public int getSessionExpireRate() {
        return 0;
    }

    @Override
    public void remove(Session session, boolean update) {
        log.info("Removing session ID : " + session.getId());
        String sql = String.format("delete from %s where %s = :id", tableDetails.getTable(), tableDetails.getIdCol());
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id", session.getId());
        jdbcTemplate.update(sql, params);
    }

}

