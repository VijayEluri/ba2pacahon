package org.gost19.ba2pacahon;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.json.simple.JSONObject;

import ru.mndsc.bigarchive.server.kernel.document.XmlUtil;
import ru.mndsc.bigarchive.server.kernel.document.beans.XmlAttribute;
import ru.mndsc.bigarchive.server.kernel.document.beans.XmlDocument;
import ru.mndsc.bigarchive.server.kernel.document.beans.XmlDocumentType;
import ru.mndsc.zdms_component.messaging.AMQPMessagingManager;
import ru.mndsc.zdms_component.messaging.IMessagingManager;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Fetcher {
	private static Properties properties = new Properties();
	private static Connection connection = null;
	private static String dbUser;
	private static String dbPassword;
	private static String dbUrl;
	private static IMessagingManager messagingManager;
	private static String host;
	private static String port;
	private static String virtualHost;
	private static String userName;
	private static String password;
	private static String queue;
	private static long responseWaitingLimit = 120000;
	private static String mongodb_host;
	private static int mongodb_port;

	private static DBCollection ba_docs_coll;

	/**
	 * Выгружает данные структуры документов в виде пользовательских онтологий
	 */

	private static void fetchDocumentTypes() throws Exception {

		ResultSet templatesRs = connection
				.createStatement()
				.executeQuery(
						"select objectId FROM objects where isTemplate = 1 and actual = 1 ORDER BY timestamp ASC");

		while (templatesRs.next()) {
			String docId = templatesRs.getString(1);
			System.out.println("templateId = [" + docId + "]");

			String docDataQuery = "select distinct content, kindOf, timestamp, recordId FROM objects where objectId = '"
					+ docId + "' order by timestamp ASC";
			Statement st1 = connection.createStatement();
			ResultSet templateRecordRs = st1.executeQuery(docDataQuery);

			while (templateRecordRs.next()) {
				try {
					String docXmlStr = templateRecordRs.getString(1);

					// int kindOf = templateRecordRs.getInt(2);

					Long timestamp = templateRecordRs.getLong(3);
					String recordId = templateRecordRs.getString(4);

					// XmlDocumentType doc = (XmlDocumentType)
					// XmlUtil.unmarshall(docXmlStr);
					// doc.setVersionId(recordId);
					// String dd = doc.toJSON();

					BasicDBObject doc1 = new BasicDBObject("id", docId)
							.append("version", recordId)
							.append("type", "TEMPLATE")
							.append("content", docXmlStr)
							.append("timestamp", new Date(timestamp))
							.append("timestamp_l", timestamp);
					ba_docs_coll.insert(doc1);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			templateRecordRs.close();
			st1.close();

		}

		templatesRs.close();

		System.out.println("conversion templates is complete");
	}

	private static int count_documents = 0;

	private static void set__count_references_document() throws Exception {
		HashMap<String, Integer> count_references_document = new HashMap<String, Integer>();

		DBCursor cursor = ba_docs_coll.find(new BasicDBObject("type",
				"DOCUMENT"));

		try {
			while (cursor.hasNext()) {
				DBObject obj = cursor.next();

				String content = (String) obj.get("content");
				String id = (String) obj.get("id");
				String version = (String) obj.get("version");
				XmlDocument doc = (XmlDocument) XmlUtil.unmarshall(content);
				doc.setVersionId(version);

				try {
					if (doc.getAuthorId() != null
							&& doc.getAuthorId().length() > 0) {
						Integer count_rd = count_references_document.get(doc
								.getAuthorId());
						if (count_rd == null)
							count_rd = 0;
						count_references_document.put(doc.getAuthorId(),
								count_rd + 1);
					}

					for (XmlAttribute attr : doc.getAttributes()) {
						String type = attr.getType();
						String link = null;

						if (type.equals("LINK")) {
							link = attr.getLinkValue();
						} else if (type.equals("ORGANIZATION")) {
							link = attr.getOrganizationValue();
						} else if (type.equals("DICTIONARY")) {
							link = attr.getRecordIdValue();
						}

						if (link != null && link.length() > 0) {
							Integer count_rd = count_references_document
									.get(link);
							if (count_rd == null)
								count_rd = 0;
							count_references_document.put(link, count_rd + 1);
						}
					}

				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		} finally {
			cursor.close();
		}

		// заполним ссчетчики ссылок на документ
		cursor = ba_docs_coll.find(new BasicDBObject("type", "DOCUMENT"));

		try {
			while (cursor.hasNext()) {
				DBObject obj = cursor.next();

				String id = (String) obj.get("id");

				Integer count_rd = count_references_document.get(id);

				if (count_rd != null && count_rd > 0) {
					try {
						ba_docs_coll.update(new BasicDBObject("id", id),
								new BasicDBObject("$set", new BasicDBObject(
										"count-references-on-document",
										count_rd)), true, false);
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}
		} finally {
			cursor.close();
		}

	}

	private static int fetchDocuments() throws Exception {
		String docsIdDataQuery;
		ResultSet docRecordsRs;

		long timestamp_start = 0;
		long timestamp_delta = 10 * 24 * 60 * 60 * 1000;

		long prev_doc_count = 0;

		while (timestamp_start < System.currentTimeMillis()) {
			docsIdDataQuery = "select objectId, recordId, content, timestamp, templateVersionId, templateVersionId "
					+ "FROM objects "
					+ "WHERE isTemplate = 0 and actual = 1 and timestamp > "
					+ timestamp_start
					+ " and timestamp <= "
					+ (timestamp_start + timestamp_delta)
					+ " ORDER BY timestamp ASC";
			docRecordsRs = connection.createStatement().executeQuery(
					docsIdDataQuery);

			while (docRecordsRs.next()) {
				String docId = docRecordsRs.getString(1);
				String docXmlStr = docRecordsRs.getString(3);
				String recordId = docRecordsRs.getString(2);
				Long timestamp = docRecordsRs.getLong(4);
				String templateId = docRecordsRs.getString(5);
				String templateVersionId = docRecordsRs.getString(6);

				BasicDBObject doc1 = new BasicDBObject("id", docId)
						.append("version", recordId)
						.append("templateId", templateId)
						.append("templateVersionId", templateVersionId)
						.append("type", "DOCUMENT")
						.append("content", docXmlStr)
						.append("timestamp", new Date(timestamp))
						.append("timestamp_l", timestamp);
				ba_docs_coll.insert(doc1);
				count_documents++;
			}
			docRecordsRs.close();

			timestamp_start += timestamp_delta;

			if (count_documents - prev_doc_count > 0)
				System.out.println("select documents, timestamp ["
						+ timestamp_start + ":"
						+ (timestamp_start + timestamp_delta) + "], count="
						+ count_documents);

			prev_doc_count = count_documents;
		}

		return count_documents;
	}

	static void init_source() throws Exception {
		System.out.print("connect to source database " + dbUrl + "...");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl,
				dbUser, dbPassword);

		System.out.println("ok");
	}

	private static void loadProperties() {

		try {
			properties.load(new FileInputStream("ba2pacahon.properties"));

			dbUser = properties.getProperty("dbUser", "ba");
			dbPassword = properties.getProperty("dbPassword", "123456");
			dbUrl = properties.getProperty("dbUrl", "localhost:3306");
			host = properties.getProperty("rabbit-host", "localhost");
			port = properties.getProperty("rabbit-port", "5672");
			virtualHost = properties.getProperty("rabbit-virtualHost",
					"bigarchive");
			userName = properties.getProperty("rabbit-userName", "ba");
			password = properties.getProperty("rabbit-password", "123456");
			queue = properties.getProperty("rabbit-queue", "new-search");
			String mongo = properties.getProperty("mongodb", "localhost:27017");
			String els[] = mongo.split(":");
			mongodb_host = els[0];
			mongodb_port = Integer.parseInt(els[1]);
		} catch (IOException e) {
			writeDefaultProperties();
		}

	}

	private static void writeDefaultProperties() {
		System.out.println("Writing default properties.");

		properties.setProperty("dbUser", "ba");
		properties.setProperty("dbPassword", "123456");
		properties.setProperty("dbUrl", "localhost:3306");
		properties.setProperty("mongodb", "localhost:27017");

		try {
			properties.store(new FileOutputStream("ba2pacahon.properties"),
					null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void toQueueDocumentTypes() throws Exception {
		BasicDBObject query = new BasicDBObject("type", "TEMPLATE");

		DBCursor cursor = ba_docs_coll.find(query);

		try {
			while (cursor.hasNext()) {
				DBObject obj = cursor.next();
				String content = (String) obj.get("content");
				String version = (String) obj.get("version");
				XmlDocumentType doc = (XmlDocumentType) XmlUtil
						.unmarshall(content);
				doc.setVersionId(version);

				String dd = doc.toJSON();
				messagingManager.sendMessage(queue, dd);

				// System.out.println(dd);
			}
		} finally {
			cursor.close();
		}
	}

	private static void toQueueDocuments(BasicDBObject query,
			String is_processed_links) throws Exception {
		DBCursor cursor = ba_docs_coll.find(query);

		try {
			while (cursor.hasNext()) {
				DBObject obj = cursor.next();

				String content = (String) obj.get("content");
				String id = (String) obj.get("id");
				String version = (String) obj.get("version");

				Object count_references_on_document_o = obj
						.get("count-references-on-document");

				int count_references_on_document = 0;

				if (count_references_on_document_o != null) {
					if (count_references_on_document_o instanceof java.lang.Integer) {
						count_references_on_document = (Integer) count_references_on_document_o;
					}
					if (count_references_on_document_o instanceof java.lang.Double) {
						count_references_on_document = ((Double) count_references_on_document_o)
								.intValue();
					}

				}

				XmlDocument doc = (XmlDocument) XmlUtil.unmarshall(content);
				doc.setVersionId(version);

				try {
					JSONObject jo = doc.getJSON();
					jo.put("is-processed-links", is_processed_links);

					if (count_references_on_document > 0)
						jo.put("is-cached", "Y");

					String dd = JSONObject.toJSONString(jo);
					messagingManager.sendMessage(queue, dd);
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				// System.out.println(dd);
			}
		} finally {
			cursor.close();
		}
	}

	public static void main(String[] args) throws Exception {
		messagingManager = new AMQPMessagingManager();
		loadProperties();

		MongoClient mongoClient = new MongoClient(mongodb_host, mongodb_port);
		DB pacahon_db = mongoClient.getDB("pacahon");

		ba_docs_coll = pacahon_db.getCollection("ba_docs");

		if (ba_docs_coll.count() == 0) {
			try {
				System.out.println("start sql ba xml -> mongo ba json");
				System.out.println("init_source");
				init_source();
				System.out.println("fetchDocumentTypes");
				fetchDocumentTypes();
				System.out.println("fetchDocuments");
				fetchDocuments();
				System.out.println("set__count_references_document");
				set__count_references_document();
				System.out.println("end start sql ba xml -> mongo ba json");
				// Thread.currentThread().sleep(999999999);
			} catch (Exception ex) {
				System.out.println("Error !");
				ex.printStackTrace(System.out);
			}

		} else {
			System.out.println("start ba json -> pacahon");
			messagingManager.init(host, Integer.parseInt(port), virtualHost,
					userName, password, responseWaitingLimit, null);
			System.out.println("TEMPLATE pass I");
			toQueueDocumentTypes();
			System.out.println("TEMPLATE pass II");
			toQueueDocumentTypes();
			System.out.println("DOCUMENTS pass I");
			toQueueDocuments(new BasicDBObject("type", "DOCUMENT"), "N");
			System.out.println("DOCUMENTS pass II");
			toQueueDocuments(new BasicDBObject("type", "DOCUMENT"), "Y");
			System.out.println("end ba json -> pacahon");
		}

	}
}
