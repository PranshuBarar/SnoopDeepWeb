package org.example;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import java.sql.*;
import java.util.Base64;

public class CrawlerBolt extends BaseBasicBolt {

    public static Connection connection;
    public static boolean table;

    //Here we will execute our logic of crawling the web
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //first we will fetch the url from the tuple
        String url = tuple.getStringByField("url");

        //Here we will set some property for the chrome driver
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--remote-allow-origins=*");
        System.setProperty("webdriver.chrome.driver","src/main/java/drivers/chromedriver.exe");

        //Now we will open an instance of the chrome web browser
        WebDriver driver = new ChromeDriver(options);

        //We will navigate to the url
        driver.navigate().to(url);

        //Here it will be better if sleep the thread for some time because due to slow internet speeds
        //some time it takes 8 to 10 seconds to load the page completely
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        //Here we will fetch the full html source code of the webpage
        String htmlData = driver.getPageSource();

        //We will take full screenshot of the webpage and convert it into base64Image string
        byte[] screenshotBytes = ((TakesScreenshot) driver).getScreenshotAs(OutputType.BYTES);
        String base64Image = Base64.getEncoder().encodeToString(screenshotBytes);
        try {
            //Now here we will send all the data to the database, so here I have called a function of
            //CrawlerDatabaseManagement class
            CrawlerDatabaseManagement.dataEntryToDatabase(htmlData, base64Image, url, url);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        basicOutputCollector.emit(new Values(url));
    }

    //This function is simple for declaring output fields
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }


}
