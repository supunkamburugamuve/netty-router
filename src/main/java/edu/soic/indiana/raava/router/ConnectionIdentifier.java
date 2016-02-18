package edu.soic.indiana.raava.router;

/**
 * The connection identifier is used primarily as a key to identify a client connection required.
 */
public class ConnectionIdentifier {
    private final String host;
    private final int port;

    public ConnectionIdentifier(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return host +":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionIdentifier that = (ConnectionIdentifier) o;

        if (port != that.port) return false;
        return !(host != null ? !host.equals(that.host) : that.host != null);

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }
}
