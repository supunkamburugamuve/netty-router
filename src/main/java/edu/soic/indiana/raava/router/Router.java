package edu.soic.indiana.raava.router;

import java.util.HashMap;
import java.util.Map;

public class Router {
    private RouterServer routerServer;
    private Map<ConnectionIdentifier, RouterClient> routerClients = new HashMap<ConnectionIdentifier, RouterClient>();


}
