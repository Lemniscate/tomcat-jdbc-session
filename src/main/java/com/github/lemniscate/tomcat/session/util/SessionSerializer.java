package com.github.lemniscate.tomcat.session.util;

import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;

import javax.servlet.http.HttpSession;
import java.io.*;

public class SessionSerializer {
	private ClassLoader loader;

	public void setClassLoader(ClassLoader loader) {
		this.loader = loader;
	}

	public byte[] serialize(HttpSession session) throws IOException {

		StandardSession standardSession = (StandardSession) session;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));
		oos.writeLong(standardSession.getCreationTime());

        Object auth = standardSession.getAttribute("SPRING_SECURITY_CONTEXT");
        oos.writeBoolean( auth != null );

        if( auth != null ){
            oos.writeObject(auth);
        }

		standardSession.writeObjectData(oos);

		oos.close();

		return bos.toByteArray();
	}

	public HttpSession deserialize(byte[] data, HttpSession session)
			throws IOException, ClassNotFoundException {

		StandardSession standardSession = (StandardSession) session;

        if( data != null && data.length > 0 ){
            BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
            ObjectInputStream ois = new CustomObjectInputStream(bis, loader);
            standardSession.setCreationTime(ois.readLong());
            boolean hasAuth = ois.readBoolean();
            if( hasAuth ){
                Object auth = ois.readObject();
                standardSession.setAttribute("SPRING_SECURITY_CONTEXT", auth);
            }
            standardSession.readObjectData(ois);
        }

		return session;
	}
}
