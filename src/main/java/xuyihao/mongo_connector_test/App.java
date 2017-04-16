package xuyihao.mongo_connector_test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import xuyihao.mongo.connector.MongoDBConnector;
import xuyihao.mongo.util.CommonUtils;

/**
 * 
 * @author Xuyh at 2016年9月26日 下午5:28:17.
 *
 */
public class App {
	public static void main(String[] args) {
		testDatabaseEncodingGetValue();
	}

	public static void testDatabaseEncodingGetValue(){
		MongoDBConnector connector = new MongoDBConnector();
		if (connector.connectWithAuthentiation("115.28.192.61", 27017, "WSN_Monitor", "xuyh", "system")){
			Document document = connector.findOne("testEncoding", new Document());
			CommonUtils.outputLine(document.toJson());
		}
	}

	public static void testDatabaseEncoding(){
		MongoDBConnector connector = new MongoDBConnector();
		if (connector.connectWithAuthentiation("115.28.192.61", 27017, "WSN_Monitor", "xuyh", "system")){
			Map<String, Object> valueMap = new HashMap<>();
			valueMap.put("name", "中文名");
			valueMap.put("target", "压强");
			valueMap.put("value", "5Pa");
			Document document = new Document(valueMap);
			connector.insrtDocument("testEncoding", document);
		}
	}

	public static void testConvertDocumentToMap() {
		MongoDBConnector connector = new MongoDBConnector();
		//boolean connectingResult = connector.connect("127.0.0.1", 27017, "test");
		boolean connectingResult = connector.connectWithAuthentiation("127.0.0.1", 27017, "admin", "admin", "admin");
		System.out.println(connectingResult);
		Bson bson = new Document("listDatabases", 1);
		Document result = connector.runCommand(bson);
		Map<String, Object> mapResult = convertDocumentToMap(result);
		for (String key : mapResult.keySet()) {
			System.out.println(key + " : " + result.get(key).toString());
		}
	}

	public static void testGettingInfomationAndReturnDocument() {
		MongoDBConnector connector = new MongoDBConnector();
		boolean connectingResult = connector.connect("127.0.0.1", 27017, "test");
		System.out.println(connectingResult);
		Bson bson = new Document("buildInfo", 1);
		Document result = connector.runCommand(bson);
		for (String key : result.keySet()) {
			System.out.println(key + " : " + result.get(key).toString());
		}
	
		Bson bson2 = new Document("serverStatus", 1);
		Document result2 = connector.runCommand(bson2);
		for (String key : result2.keySet()) {
			System.out.println(key + " : " + result2.get(key).toString());
		}
	}

	public static Map<String, Object> convertDocumentToMap(Document document) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : document.keySet()) {
			Object value = document.get(key);
			if (value instanceof Document) {
				map.put(key, convertDocumentToMap((Document) value));
			} else if(value instanceof List){
				@SuppressWarnings("unchecked")
				List<Object> valueList = (List<Object>)value;
				List<Object> objects = new ArrayList<Object>();
				for(Object v : valueList){
					if(v instanceof Document){
						objects.add(convertDocumentToMap((Document) v));
					}else{
						objects.add(v);
					}
				}
				map.put(key, objects);
			}else {
				map.put(key, value);
			}
		}
		return map;
	}
}
