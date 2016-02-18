package edu.soic.indiana.raava.router.netty;

import edu.soic.indiana.raava.router.RouterClientHandler;
import edu.soic.indiana.raava.router.RouterMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class NettyClient {
    private static Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    protected String name;

    protected final int maxRetries;
    protected final int baseSleepMs;
    protected final int maxSleepMs;
    protected final long timeoutMs;
    protected final int MAX_SEND_PENDING;

    protected AtomicInteger retries;

    protected AtomicReference<Channel> channelRef;
    protected ClientBootstrap bootstrap;
    protected final InetSocketAddress remoteAddr;
    protected final ChannelFactory factory;

    protected final int buffer_size;
    protected final AtomicBoolean beingClosed;

    protected AtomicLong pendings;
    protected int messageBatchSize;

    protected ScheduledExecutorService scheduler;

    protected String address;

    protected ReconnectRunnable reconnector;
    protected ChannelFactory clientChannelFactory;

    protected Set<Channel> closingChannel;

    protected AtomicBoolean isConnecting = new AtomicBoolean(false);

    protected NettyConnection nettyConnection;

    protected Map stormConf;

    protected boolean connectMyself;

    protected Object channelClosing = new Object();

    @SuppressWarnings("rawtypes")
    NettyClient(Map storm_conf, ChannelFactory factory,
                ScheduledExecutorService scheduler, String host, int port,
                ReconnectRunnable reconnect) {
        this.stormConf = storm_conf;
        this.factory = factory;
        this.scheduler = scheduler;
        this.reconnector = reconnect;

        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        beingClosed = new AtomicBoolean(false);
        pendings = new AtomicLong(0);

        nettyConnection = new NettyConnection();
        nettyConnection.setClientPort(NetWorkUtils.ip(),
                ConfigExtension.getLocalWorkerPort(storm_conf));
        nettyConnection.setServerPort(host, port);

        // Configure
        buffer_size =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        maxRetries =
                Math.min(30, Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        baseSleepMs =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        maxSleepMs =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

        timeoutMs = ConfigExtension.getNettyPendingBufferTimeout(storm_conf);
        MAX_SEND_PENDING =
                (int) ConfigExtension.getNettyMaxSendPending(storm_conf);

        this.messageBatchSize =
                Utils.getInt(
                        storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE),
                        262144);
        messageBatchRef = new AtomicReference<MessageBatch>();

        // Start the connection attempt.
        remoteAddr = new InetSocketAddress(host, port);
        name = remoteAddr.toString();
        connectMyself = isConnectMyself(stormConf, host, port);

        address = JStormServerUtils.getName(host, port);

        closingChannel = new HashSet<Channel>();
    }

    class StormClientPipelineFactory implements ChannelPipelineFactory {
        private NettyClient client;
        private Map         conf;

        StormClientPipelineFactory(NettyClient client, Map conf) {
            this.client = client;
            this.conf = conf;

        }

        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();

            // Decoder
            pipeline.addLast("decoder", new MessageDecoder(false, conf));
            // Encoder
            pipeline.addLast("encoder", new MessageEncoder());
            // business logic.
            pipeline.addLast("handler", new RouterClientHandler(client));

            return pipeline;
        }
    }


    public void start() {
        bootstrap = new ClientBootstrap(clientChannelFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("reuserAddress", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this, stormConf));
        reconnect();
    }

    public boolean isConnectMyself(Map conf, String host, int port) {
        String localIp = NetWorkUtils.ip();
        String remoteIp = NetWorkUtils.host2Ip(host);
        int localPort = ConfigExtension.getLocalWorkerPort(conf);

        if (localPort == port &&
                localIp.equals(remoteIp)) {
            return true;
        }

        return false;
    }

    /**
     * The function can't be synchronized, otherwise it will be deadlock
     *
     */
    public void doReconnect() {
        if (channelRef.get() != null) {
            return;
        }

        if (isClosed()) {
            return;
        }

        if (isConnecting.getAndSet(true)) {
            LOG.info("Connect twice {}", name);
            return;
        }

        long sleepMs = getSleepTimeMs();
        LOG.info("Reconnect ... [{}], {}, sleep {}ms", retries.get(), name, sleepMs);
        ChannelFuture future = bootstrap.connect(remoteAddr);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                isConnecting.set(false);
                Channel channel = future.getChannel();
                if (future.isSuccess()) {
                    // do something else
                    LOG.info("Connection established, channel = :{}", channel);
                    setChannel(channel);
                } else {
                    LOG.info("Failed to reconnect ... [{}], {}, channel = {}, cause = {}",
                            retries.get(), name, channel, future.getCause());
                    reconnect();
                }
            }
        });
        Utils.sleepMs(sleepMs);
    }

    public void reconnect() {
        reconnector.pushEvent(this);
    }

    public boolean isClosed() {
        return beingClosed.get();
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private int getSleepTimeMs() {
        int sleepMs = baseSleepMs * retries.incrementAndGet();
        if (sleepMs > 1000) {
            sleepMs = 1000;
        }
        return sleepMs;
    }

    /**
     * Enqueue a task message to be sent to server
     */
    public void send(List<RouterMessage> messages) {
        LOG.warn("Should be overload");
    }

    public void send(RouterMessage message) {
        LOG.warn("Should be overload");
    }

    Channel isChannelReady() {
        Channel channel = channelRef.get();
        if (channel == null) {
            return null;
        }

        // improve performance skill check
        if (channel.isWritable() == false) {
            return null;
        }

        return channel;
    }

    protected synchronized void flushRequest(Channel channel,
                                             final MessageBatch requests) {
        if (requests == null || requests.isEmpty())
            return;

        Double batchSize = Double.valueOf(requests.getEncoded_length());
        pendings.incrementAndGet();
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                pendings.decrementAndGet();
                if (!future.isSuccess()) {
                    Channel channel = future.getChannel();
                    if (!isClosed()) {
                        LOG.info("Failed to send requests to " + name + ": "
                                + channel.toString() + ":", future.getCause());
                    }

                    if (null != channel) {

                        exceptionChannel(channel);
                    }
                }
            }
        });
    }

    /**
     * gracefully close this client.
     *
     * We will send all existing requests, and then invoke close_n_release()
     * method
     */
    public void close() {
        LOG.info("Close netty connection to {}", name());
        if (beingClosed.compareAndSet(false, true) == false) {
            LOG.info("Netty client has been closed.");
            return;
        }

        Channel channel = channelRef.get();
        if (channel == null) {
            LOG.info("Channel {} has been closed before", name());
            return;
        }

        if (channel.isWritable()) {
            MessageBatch toBeFlushed = messageBatchRef.getAndSet(null);
            flushRequest(channel, toBeFlushed);
        }

        // wait for pendings to exit
        final long timeoutMilliSeconds = 10 * 1000;
        final long start = System.currentTimeMillis();

        LOG.info("Waiting for pending batchs to be sent with " + name()
                        + "..., timeout: {}ms, pendings: {}", timeoutMilliSeconds,
                pendings.get());

        while (pendings.get() != 0) {
            try {
                long delta = System.currentTimeMillis() - start;
                if (delta > timeoutMilliSeconds) {
                    LOG.error(
                            "Timeout when sending pending batchs with {}..., there are still {} pending batchs not sent",
                            name, pendings.get());
                    break;
                }
                Thread.sleep(1000); // sleep 1s
            } catch (InterruptedException e) {
                break;
            }
        }

        close_n_release();

    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    void close_n_release() {
        if (channelRef.get() != null) {
            setChannel(null);
        }

    }

    /**
     * Avoid channel double close
     *
     * @param channel
     */
    void closeChannel(final Channel channel) {
        synchronized (channelClosing) {
            if (closingChannel.contains(channel)) {
                LOG.info(channel.toString() + " is already closed");
                return;
            }

            closingChannel.add(channel);
        }

        LOG.debug(channel.toString() + " begin to closed");
        ChannelFuture closeFuture = channel.close();
        closeFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                synchronized (channelClosing) {
                    closingChannel.remove(channel);
                }
                LOG.debug(channel.toString() + " finish closed");
            }
        });
    }

    void disconnectChannel(Channel channel) {
        if (isClosed()) {
            return;
        }

        if (channel == channelRef.get()) {
            setChannel(null);
            reconnect();
        } else {
            closeChannel(channel);
        }

    }

    void exceptionChannel(Channel channel) {
        if (channel == channelRef.get()) {
            setChannel(null);
        } else {
            closeChannel(channel);
        }
    }

    void setChannel(Channel newChannel) {
        final Channel oldChannel = channelRef.getAndSet(newChannel);

        if (newChannel != null) {
            retries.set(0);
        }

        final String oldLocalAddres =
                (oldChannel == null) ? "null" : oldChannel.getLocalAddress()
                        .toString();
        String newLocalAddress =
                (newChannel == null) ? "null" : newChannel.getLocalAddress()
                        .toString();
        LOG.info("Use new channel {} replace old channel {}", newLocalAddress,
                oldLocalAddres);

        // avoid one netty client use too much connection, close old one
        if (oldChannel != newChannel && oldChannel != null) {
            closeChannel(oldChannel);
        }
    }

}

