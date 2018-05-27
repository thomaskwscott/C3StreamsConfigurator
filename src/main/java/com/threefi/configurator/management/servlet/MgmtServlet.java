package com.threefi.configurator.management.servlet;

import com.threefi.configurator.ClientExtractor;
import com.threefi.configurator.MessageRegenerator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class MgmtServlet extends HttpServlet {

    private String bootstrapServer = "localhost:10091";
    private String monitoringTopic = "_confluent-monitoring";



    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        if(request.getParameter("monitorName") != null)
        {
            String out = parseAndRun(request);
            response.getWriter().println(out);
            return;
        }

        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        StringBuilder builder = new StringBuilder();
        builder.append("<html><body><h2>Monitor Maker</h2><form form=\"/\">");
        builder.append("<head><style>table, td { width:100%; border: 1px solid black; }</style></head>");
        builder.append("<table>");
        builder.append("<tr><td>Monitor Name:</td><td><input type=\"text\" name=\"monitorName\" value=\"monitor1\"></td></tr>");
        builder.append("</table>");
        builder.append("<table><tr><th>Client</th><th>Source</th><th>Sink</th></tr>");

        for(String client : ClientExtractor.FOUND_CLIENTS)
        {
            builder.append("<tr><td>" + client + "</td>" );
            builder.append("<td><input type=\"checkbox\" id=\"" + client + "_source\" name=\"" + client + "_source\" value=\"true\"></td>" );
            builder.append("<td><input type=\"checkbox\" id=\"" + client + "_sink\" name=\"" + client + "_sink\" value=\"true\"></td>" );
            builder.append("<tr>");
        }
        builder.append("</table>");

        builder.append("<h3>Topic Mappings:");
        builder.append("<table>");
        builder.append("<tr><td><input type=\"text\" name=\"topicLeft1\" value=\"leftTopic1\"></td><td>maps to</td><td><input type=\"text\" name=\"topicRight1\" value=\"rightTopic1\"></td></tr>");
        builder.append("<tr><td><input type=\"text\" name=\"topicLeft2\" value=\"leftTopic2\"></td><td>maps to</td><td><input type=\"text\" name=\"topicRight2\" value=\"rightTopic2\"></td></tr>");
        builder.append("<tr><td><input type=\"text\" name=\"topicLeft3\" value=\"leftTopic3\"></td><td>maps to</td><td><input type=\"text\" name=\"topicRight3\" value=\"rightTopic3\"></td></tr>");
        builder.append("</table>");

        builder.append("<input type=\"submit\" value=\"Submit\">");

        builder.append("</body></html>");


        response.getWriter().println(builder.toString());
    }

    protected String parseAndRun(HttpServletRequest req)
    {
        StringBuilder sourceBuilder = new StringBuilder();
        StringBuilder sinkBuilder = new StringBuilder();
        StringBuilder topicMappingBuilder = new StringBuilder();

        for(String client : ClientExtractor.FOUND_CLIENTS)
        {
            if(req.getParameter(client + "_source") != null)
            {
                sourceBuilder.append(client + ",");
            }
            if(req.getParameter(client + "_sink") != null)
            {
                sinkBuilder.append(client + ",");
            }
        }
        for(int i=1;i<=3;i++)
        {
            if(!req.getParameter( "topicLeft" + i).equals("") )
            {
                topicMappingBuilder.append(req.getParameter( "topicLeft" + i) + "=" + req.getParameter( "topicRight" + i) + "," );
            }
        }

        String monitorName = req.getParameter("monitorName");

        Runnable regenerator = new Runnable() {
            @Override
            public void run() {
                MessageRegenerator.main(new String[] {
                        bootstrapServer,
                        monitoringTopic,
                        monitorName,
                        sourceBuilder.toString().substring(0,sourceBuilder.length()-1),
                        sinkBuilder.toString().substring(0,sinkBuilder.length()-1),
                        topicMappingBuilder.toString().substring(0,topicMappingBuilder.length()-1)
                });
            }
        };

        new Thread(regenerator).start();

        return "Parameters: " +
            bootstrapServer + " " +
            monitoringTopic + " " +
            monitorName + " " +
            sourceBuilder.toString().substring(0,sourceBuilder.length()-1) + " " +
            sinkBuilder.toString().substring(0,sinkBuilder.length()-1) + " " +
            topicMappingBuilder.toString().substring(0,topicMappingBuilder.length()-1);

    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public void setMonitoringTopic(String monitoringTopic) {
        this.monitoringTopic = monitoringTopic;
    }
}
