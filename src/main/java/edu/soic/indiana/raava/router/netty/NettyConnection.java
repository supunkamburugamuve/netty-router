package edu.soic.indiana.raava.router.netty;

import java.io.Serializable;

public class NettyConnection implements Serializable {
    protected String clientPort;
    protected String serverPort;

    public String getClientPort() {
        return clientPort;
    }

    public void setClientPort(String client, int port) {
        String ip = NetWorkUtils.host2Ip(client);
        clientPort = ip + ":" + port;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String server, int port) {
        String ip = NetWorkUtils.host2Ip(server);
        serverPort = ip + ":" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result =
                prime * result
                        + ((clientPort == null) ? 0 : clientPort.hashCode());
        result =
                prime * result
                        + ((serverPort == null) ? 0 : serverPort.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NettyConnection other = (NettyConnection) obj;
        if (clientPort == null) {
            if (other.clientPort != null)
                return false;
        } else if (!clientPort.equals(other.clientPort))
            return false;
        if (serverPort == null) {
            if (other.serverPort != null)
                return false;
        } else if (!serverPort.equals(other.serverPort))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return clientPort  + "->" + serverPort;
    }

    public static String mkString(String client, int clientPort,
                                  String server, int serverPort) {
        return client + ":" + clientPort + "->" + server + ":" + serverPort;
    }

}
