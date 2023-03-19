package org.example;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {
        CrawlerTopology crawlerTopology = new CrawlerTopology();
        crawlerTopology.crawlerTopologyInitializer();
        Scanner sc = new Scanner(System.in);
        int id = sc.nextInt();
        CrawlerDatabaseManagement.dataRetrieval(id);
    }
}
