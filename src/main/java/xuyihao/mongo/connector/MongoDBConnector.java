package xuyihao.mongo.connector;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDBConnector {
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;

	public MongoDBConnector() {

	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	/**
	 * 连接数据库
	 * 
	 * @param ip
	 * @param port
	 * @param dbName
	 * @return
	 */
	public boolean connect(String ip, int port, String dbName) {
		try {
			ServerAddress addr = new ServerAddress(ip, port);
			MongoClientOptions.Builder builder = MongoClientOptions.builder();
			builder.connectionsPerHost(300);
			builder.connectTimeout(15000);
			builder.maxWaitTime(5000);
			builder.socketTimeout(0);
			builder.threadsAllowedToBlockForConnectionMultiplier(5000);
			MongoClientOptions options = builder.build();
			mongoClient = new MongoClient(addr, options);
			mongoDatabase = mongoClient.getDatabase(dbName);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * 用户名密码连接数据库
	 * 
	 * @param ip
	 * @param port
	 * @param dbName
	 * @param userName
	 * @param password
	 * @return
	 */
	public boolean connectWithAuthentiation(String ip, int port, String dbName, String userName, String password) {
		try {
			ServerAddress serverAddress = new ServerAddress(ip, port);
			List<ServerAddress> address = new ArrayList<ServerAddress>();
			address.add(serverAddress);
			MongoCredential mongoCredential = MongoCredential.createCredential(userName, dbName, password.toCharArray());
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(mongoCredential);
			MongoClientOptions.Builder builder = MongoClientOptions.builder();
			builder.connectionsPerHost(300);
			builder.connectTimeout(15000);
			builder.maxWaitTime(5000);
			builder.socketTimeout(0);
			builder.threadsAllowedToBlockForConnectionMultiplier(5000);
			MongoClientOptions options = builder.build();
			mongoClient = new MongoClient(address, credentials, options);
			mongoDatabase = mongoClient.getDatabase(dbName);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * 运行命令
	 * 
	 * @param command
	 */
	public Document runCommand(Bson command) {
		if (mongoDatabase != null) {
			Document document = mongoDatabase.runCommand(command);
			return document;
		} else {
			return null;
		}
	}

	/**
	 * 创建集合
	 * 
	 * @param collectionName
	 * @return
	 */
	public boolean createCollection(String collectionName) {
		if (mongoDatabase != null) {
			mongoDatabase.createCollection(collectionName);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 获取集合
	 * 
	 * @param collectionName
	 * @return
	 */
	public MongoCollection<Document> getCollection(String collectionName) {
		if (mongoDatabase != null) {
			return mongoDatabase.getCollection(collectionName);
		} else {
			return null;
		}
	}

	/**
	 * 插入文档
	 * 
	 * @param collectionName
	 * @param document
	 * @return
	 */
	public boolean insrtDocument(String collectionName, Document document) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.insertOne(document);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 插入多个文档
	 * 
	 * @param collectionName
	 * @param documents
	 * @return
	 */
	public boolean insertDocumentMany(String collectionName, List<Document> documents) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.insertMany(documents);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 获取所有文档
	 * 
	 * @param collectionName
	 * @return
	 */
	public MongoCursor<Document> find(String collectionName) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			FindIterable<Document> findIterable = collection.find();
			return findIterable.iterator();
		} else {
			return null;
		}
	}

	/**
	 * 查询一条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @return
	 */
	public Document findOne(String collectionName, Bson filter) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			FindIterable<Document> findIterable = collection.find(filter);
			return findIterable.iterator().next();
		} else {
			return null;
		}
	}

	/**
	 * 查询多条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @return
	 */
	public MongoCursor<Document> findMany(String collectionName, Bson filter) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			FindIterable<Document> findIterable = collection.find(filter);
			return findIterable.iterator();
		} else {
			return null;
		}
	}

	/**
	 * 更新一条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @param update
	 * @return
	 */
	public boolean updateFirst(String collectionName, Bson filter, Bson update) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.updateOne(filter, update);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 更新多条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @param update
	 * @return
	 */
	public boolean updateMany(String collectionName, Bson filter, Bson update) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.updateMany(filter, update);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 删除一条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @return
	 */
	public boolean deleteOne(String collectionName, Bson filter) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.deleteOne(filter);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 删除多条记录
	 * 
	 * @param collectionName
	 * @param filter
	 * @return
	 */
	public boolean deleteMany(String collectionName, Bson filter) {
		if (mongoDatabase != null) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
			collection.deleteMany(filter);
			return true;
		} else {
			return false;
		}
	}

}