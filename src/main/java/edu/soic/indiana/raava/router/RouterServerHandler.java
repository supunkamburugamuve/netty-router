package edu.soic.indiana.raava.router;

import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import java.util.Map;

public class RouterServerHandler extends SimpleChannelUpstreamHandler {
    private Map<ConnectionIdentifier, RouterClient> routerClients;

    public RouterServerHandler(RouterServer server, Map<ConnectionIdentifier, RouterClient> clients) {
        this.routerClients = clients;
    }
}
