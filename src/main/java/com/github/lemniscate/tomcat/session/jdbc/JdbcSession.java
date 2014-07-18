package com.github.lemniscate.tomcat.session.jdbc;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

public class JdbcSession extends StandardSession {

	private static final long serialVersionUID = 1L;
	private boolean isValid = true;

	public JdbcSession(Manager manager) {
		super(manager);
	}

    @Override
    public void setValid(boolean isValid) {
        this.isValid = isValid;
        if (!isValid) {
            String keys[] = keys();
            for (String key : keys) {
                removeAttributeInternal(key, false);
            }
            getManager().remove(this);

        }
    }

    @Override
	protected boolean isValidInternal() {
		return isValid;
	}

	@Override
	public boolean isValid() {
		return isValidInternal();
	}

	@Override
	public void invalidate() {
		setValid(false);
	}

	@Override
	public void setId(String id) {
		this.id = id;
	}
}
