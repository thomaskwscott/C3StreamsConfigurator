package com.threefi.configurator.management;

import com.threefi.configurator.ClientExtractor;
import com.threefi.configurator.management.servlet.MgmtServlet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;

public class MgmtServer {

    private Server server;

    public void start() throws Exception {
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8090);
        server.setConnectors(new Connector[]{connector});

    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(Integer.valueOf(args[2]) );

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);

        handler.addServletWithMapping(MgmtServlet.class, "/*");

        ClientExtractor clientExtractor = new ClientExtractor();
        clientExtractor.main(args);


        server.start();

        server.join();
    }
}
