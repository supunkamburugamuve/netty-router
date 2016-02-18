package edu.soic.indiana.raava.router;

import edu.soic.indiana.raava.router.netty.MessageDecoder;
import edu.soic.indiana.raava.router.netty.MessageEncoder;
import edu.soic.indiana.raava.router.netty.Utils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class RouterServer {
    public static final String ROUTER_BUFFER_SIZE = "router.buffer.size";
    public static final String ROUTER_WORKER_THREADS = "router.worker.threads";

    private volatile ChannelGroup allChannels = new DefaultChannelGroup("raava-router");
    private final ChannelFactory factory;
    private final ServerBootstrap bootstrap;

    public RouterServer(Map conf, int port, RouterServerHandler serverHandler) {
        int buffer_size = Utils.getInt(conf.get(ROUTER_BUFFER_SIZE));
        int maxWorkers = Utils.getInt(conf.get(ROUTER_WORKER_THREADS));

        ThreadFactory bossFactory = new CustomThreadFactory("server" + "-boss");
        ThreadFactory workerFactory = new CustomThreadFactory("server" + "-worker");
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
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(conf, serverHandler));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);
    }

    private class StormServerPipelineFactory implements ChannelPipelineFactory {
        private RouterServerHandler serverHandler;
        private Map conf;

        StormServerPipelineFactory(Map conf, RouterServerHandler serverHandler) {
            this.conf = conf;
            this.serverHandler = serverHandler;
        }

        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", new MessageDecoder(true, conf));
            pipeline.addLast("encoder", new MessageEncoder());
            pipeline.addLast("handler", serverHandler);
            return pipeline;
        }
    }
}
