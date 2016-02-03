package edu.soic.indiana.raava.router.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class MessageDecoder extends FrameDecoder {
    @Override
    protected Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, ChannelBuffer buf) throws Exception {
        // Make sure that we have received at least a short
        long available = buf.readableBytes();
        // Length of control message is 10.
        // Minimum length of a task message is 6(short taskId, int length).
        if (available < 10) {
            // need more data
            return null;
        }

        // Mark the current buffer position before reading task/len field
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();
        // read the short field
        short code = buf.readShort();
        available -= 2;

        // case 1: Control message
        ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
        if (ctrl_msg != null) {
            if (available < 12) {
                // The time stamp bytes were not received yet - return null.
                buf.resetReaderIndex();
                return null;
            }
            long timeStamp = buf.readLong();
            int clientPort = buf.readInt();
            available -= 12;
            if (ctrl_msg == ControlMessage.EOB_MESSAGE) {

                long interval = System.currentTimeMillis() - timeStamp;
                if (interval > 0) {

                    Histogram netTransTime =
                            getTransmitHistogram(channel, clientPort);
                    if (netTransTime != null) {
                        netTransTime.update(interval );

                    }
                }

                recvSpeed.update(Double.valueOf(ControlMessage
                        .encodeLength()));
            }

            return ctrl_msg;
        }

        // case 2: task Message
        // Make sure that we have received at least an integer (length)
        if (available < 8) {
            // need more data
            buf.resetReaderIndex();

            return null;
        }

        // Read the length field.
        int length = buf.readInt();
        if (length <= 0) {
            throw new Exception("Receive one message whose TaskMessage's message length is " + length);
            return new RouterMessage(code, null);
        }
        int headerLength = buf.readInt();
        if (headerLength <= 0) {
            throw new Exception("Receive one message whose TaskMessage's message header length is " + length);
        }
        // Make sure if there's enough bytes in the buffer.
        available -= 8;
        if (available < length + headerLength) {
            // The whole bytes were not received yet - return null.
            buf.resetReaderIndex();

            return null;
        }

        int sourceTask = -1;
        String stream = null;
        if (headerLength > 0) {
            ChannelBuffer header = buf.readBytes(headerLength);
            String headerValue = new String(header.array());
            String splits[] = headerValue.split(" ");
            stream = splits[0];
            sourceTask = Integer.parseInt(splits[1]);
        }

        // There's enough bytes in the buffer. Read it.
        ChannelBuffer payload = buf.readBytes(length);

        // Successfully decoded a frame.
        // Return a TaskMessage object
        byte[] rawBytes = payload.array();
        RouterMessage ret = new RouterMessage(code, rawBytes, sourceTask, stream);
        return ret;
    }
}
