package org.example;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class CrawlerDatabaseManagement {
    public static Connection connection;

    //Here we will create a connection with the database, if already intialized we will just return the
    //same old connection
    public static Connection getConnection() throws SQLException {
        if(connection==null){
            connection = DriverManager.getConnection("jdbc:mysql://localhost/crawlerdatabase",
                    "root", "pranshubarar22@");
        }
        return connection;
    }

    //Here we will use that connection to send our data in the database
    public static void dataEntryToDatabase(String htmlData, String base64Image, String currUrl, String seedUrl) throws SQLException {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "INSERT INTO crawlerrepo (seed_url, current_url, html, base64_image) VALUES (?, ?, ?, ?)";
        preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, seedUrl);
        preparedStatement.setString(2, currUrl);
        preparedStatement.setString(3, htmlData);
        preparedStatement.setString(4, base64Image);
        preparedStatement.executeUpdate();
    }

    public static void dataRetrieval(int id) throws SQLException, IOException {
        Connection connection = getConnection();
        String query = "SELECT * FROM crawlerrepo  WHERE id = " + id;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();
        //Here we will create a JSON array and populate it with the fetched data
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("seed_url", resultSet.getString(2));
        jsonObject.put("current_url", resultSet.getString(3));
        jsonObject.put("html", resultSet.getString(4));
        jsonObject.put("base64_image", resultSet.getString(5));


        // Write the JSON array to a file
        FileWriter fileWriter = new FileWriter("src/main/resources/output.json");
        fileWriter.write(jsonObject.toJSONString());
        fileWriter.flush();
        fileWriter.close();

        // Close the database connection
        connection.close();
    }
}
