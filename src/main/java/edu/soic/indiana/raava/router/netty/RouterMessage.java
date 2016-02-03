package edu.soic.indiana.raava.router.netty;

import java.nio.ByteBuffer;

public class RouterMessage {
    private int task;
    private byte[] message;
    private int sourceTask;
    private String stream;

    public RouterMessage(int task, byte[] message) {
        this.task = task;
        this.message = message;
    }

    public RouterMessage(int _task, byte[] _message, int sourceTask, String stream) {
        this.task = _task;
        this.message = _message;
        this.sourceTask = sourceTask;
        this.stream = stream;
    }

    public int sourceTask() {
        return sourceTask;
    }

    public String stream() {
        return stream;
    }

    public int task() {
        return task;
    }

    public byte[] message() {
        return message;
    }

    public static boolean isEmpty(RouterMessage message) {
        if (message == null) {
            return true;
        } else if (message.message() == null) {
            return true;
        } else if (message.message().length == 0) {
            return true;
        }
        return false;
    }

    @Deprecated
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(message.length + 2);
        bb.putShort((short) task);
        bb.put(message);
        return bb;
    }

    @Deprecated
    public void deserialize(ByteBuffer packet) {
        if (packet == null)
            return;
        task = packet.getShort();
        message = new byte[packet.limit() - 2];
        packet.get(message);
    }
}
