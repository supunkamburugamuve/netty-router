package edu.soic.indiana.raava.router.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

public enum ControlMessage {
    EOB_MESSAGE((short) -201), OK_RESPONSE((short) -200);

    private short code;
    private long timeStamp;
    protected static int port;

    static public void setPort(int port) {
        ControlMessage.port = port;
    }

    // private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    /**
     * Return a control message per an encoded status code
     *
     * @param encoded
     * @return
     */
    static ControlMessage mkMessage(short encoded) {
        for (ControlMessage cm : ControlMessage.values()) {
            if (encoded == cm.code)
                return cm;
        }
        return null;
    }

    static int encodeLength() {
        return 14; // short + long + int
    }

    /**
     * encode the current Control Message into a channel buffer
     *
     * @throws Exception
     */
    ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout =
                new ChannelBufferOutputStream(
                        ChannelBuffers.directBuffer(encodeLength()));
        write(bout);
        bout.close();
        return bout.buffer();
    }

    void write(ChannelBufferOutputStream bout) throws Exception {
        bout.writeShort(code);
        bout.writeLong(System.currentTimeMillis());
        bout.writeInt(port);
    }

    long getTimeStamp() {
        return timeStamp;
    }

    void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}

