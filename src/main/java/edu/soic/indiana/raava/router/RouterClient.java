package edu.soic.indiana.raava.router;

import edu.soic.indiana.raava.router.netty.Utils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RouterClient {
    private static Logger LOG = LoggerFactory.getLogger(RouterClient.class);

    public static final String MAX_RETRIES = "router.conn.max.retries";
    public static final String MAX_SLEEP = "router.conn.max.sleep";
    public static final String TIMEOUTS = "router.conn.timeouts";
    public static final String BASE_SLEEP = "router.conn.base.sleep";

    protected final int maxRetries;
    protected final int baseSleepMs;
    protected final int maxSleepMs;
    protected final long timeoutMs;
    protected final ConnectionIdentifier identifier;
    protected AtomicReference<Channel> channelRef;

    private Lock channelClosingLock = new ReentrantLock();

    private Set<Channel> closingChannel = new HashSet<Channel>();

    public RouterClient(Map conf, ConnectionIdentifier identifier) {
        this.identifier = identifier;

        this.maxRetries = Utils.getInt(conf.get(MAX_RETRIES));
        this.baseSleepMs = Utils.getInt(conf.get(BASE_SLEEP));
        this.maxSleepMs = Utils.getInt(conf.get(MAX_SLEEP));
        this.timeoutMs = Utils.getInt(conf.get(TIMEOUTS));
    }

    public void send(RouterMessage message) {

    }

    protected synchronized void flushRequest(Channel channel, final RouterMessage requests) {
        if (requests == null) {
            return;
        }
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if (!future.isSuccess()) {
                    Channel channel = future.getChannel();
                    if (!isClosed()) {
                        LOG.warn("Failed to send requests to " + identifier.toString() +
                                ": " + channel.toString() + ":", future.getCause());
                    }

                    if (null != channel) {
                        exceptionChannel(channel);
                    }
                }
            }
        });
    }

    protected boolean isClosed() {
        return false;
    }

    private void exceptionChannel(Channel channel) {
        if (channel == channelRef.get()) {
            setChannel(null);
        } else {
            closeChannel(channel);
        }
    }

    private void closeChannel(final Channel channel) {
        channelClosingLock.lock();
        try {
            if (closingChannel.contains(channel)) {
                LOG.info(channel.toString() + " is already closed");
                return;
            }
        } finally {
            channelClosingLock.unlock();
        }

        LOG.debug(channel.toString() + " begin to closed");
        ChannelFuture closeFuture = channel.close();
        closeFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                channelClosingLock.lock();
                try {
                    closingChannel.remove(channel);
                    LOG.debug(channel.toString() + " finish closed");
                } finally {
                    channelClosingLock.unlock();
                }
            }
        });
    }

    private void setChannel(Channel newChannel) {
        final Channel oldChannel = channelRef.getAndSet(newChannel);
        if (newChannel != null) {
            retries.set(0);
        }
        final String oldLocalAddres =
                (oldChannel == null) ? "null" : oldChannel.getLocalAddress().toString();
        String newLocalAddress =
                (newChannel == null) ? "null" : newChannel.getLocalAddress().toString();
        LOG.info("Use new channel {} replace old channel {}", newLocalAddress, oldLocalAddres);
        // avoid one netty client use too much connection, close old one
        if (oldChannel != newChannel && oldChannel != null) {
            closeChannel(oldChannel);
            LOG.info("Successfully close old channel " + oldLocalAddres);
        }
    }
}
