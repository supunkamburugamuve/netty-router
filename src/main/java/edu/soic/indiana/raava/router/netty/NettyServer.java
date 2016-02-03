package edu.soic.indiana.raava.router.netty;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NettyServer {
    public static final String ROUTER_BUFFER_SIZE = "router.buffer.size";
    public static final String ROUTER_WORKER_THREADS = "router.worker.threads";

    final Map conf;
    final int port;
    volatile ChannelGroup allChannels = new DefaultChannelGroup("raava-router");
    final ChannelFactory factory;
    final ServerBootstrap bootstrap;

    NettyServer(Map conf, int port, boolean isSyncMode) {
        this.conf = conf;
        this.port = port;

        int buffer_size = Utils.getInt(conf.get(ROUTER_BUFFER_SIZE));
        int maxWorkers = Utils.getInt(conf.get(ROUTER_WORKER_THREADS));

        ThreadFactory bossFactory = new NettyThreadFactory("server" + "-boss");
        ThreadFactory workerFactory = new NettyThreadFactory("server" + "-worker");
        if (maxWorkers > 0) {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory));
        }

        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("reuserAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.receiveBufferSize", buffer_size);
        bootstrap.setOption("child.keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(this, conf));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);
    }

    class StormServerPipelineFactory implements ChannelPipelineFactory {
        private NettyServer server;
        private Map conf;

        StormServerPipelineFactory(NettyServer server, Map conf) {
            this.server = server;
            this.conf = conf;
        }

        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();

            // Decoder
            pipeline.addLast("decoder", new MessageDecoder(true, conf));
            // Encoder
            pipeline.addLast("encoder", new MessageEncoder());
            // business logic.
            pipeline.addLast("handler", new RouterServerHandler(server));

            return pipeline;
        }
    }



}
