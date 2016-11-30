package xuyihao.mongo_connector_test;

import org.bson.Document;

import xuyihao.mongo.connector.MongoDBConnector;
import xuyihao.mongo.util.CommonUtils;

public class AppTest{
	public static void main(String[] args) {
		MongoDBConnector connector = new MongoDBConnector();
		//connector.connectWithAuthentiation("10.1.11.235", 7201, "cmdb", "crab", "uyunsoft123");
		//connector.connectWithAuthentiationByURL("115.28.192.61", 27017, "root", "root", "test");
		//connector.connectWithAuthentiation("115.28.192.61", 27017, "root", "root", "test");
		//connector.connect("127.0.0.1", 27017, "test");
		connector.connect("115.28.192.61", 27017, "test");
		Document document = connector.runCommand(new Document("listDatabases", 1));
		CommonUtils.outputLine(document.toJson());
	}
}
