package org.gost19.ba2pacahon;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import org.json.simple.JSONArray;
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

public class Fetcher
{
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

	private static String ba_xml_mongodb_host;
	private static int ba_xml_mongodb_port;

	private static String mongodb_host;
	private static int mongodb_port;

	private static String az_mongodb_host;
	private static int az_mongodb_port;

	private static DBCollection ba_xml_coll;
	private static DBCollection simple_coll;

	/**
	 * Выгружает данные структуры документов в виде пользовательских онтологий
	 */

	private static void fetchDocumentTypes(long timestamp_start) throws Exception
	{

		ResultSet templatesRs = connection.createStatement().executeQuery(
				"select objectId FROM objects where isTemplate = 1 and actual = 1 and timestamp > " + timestamp_start
						+ " ORDER BY timestamp ASC");

		while (templatesRs.next())
		{
			String docId = templatesRs.getString(1);
			System.out.println("templateId = [" + docId + "]");

			String docDataQuery = "select distinct content, kindOf, timestamp, recordId FROM objects where objectId = '" + docId
					+ "' order by timestamp ASC";
			Statement st1 = connection.createStatement();
			ResultSet templateRecordRs = st1.executeQuery(docDataQuery);

			while (templateRecordRs.next())
			{
				try
				{
					String docXmlStr = templateRecordRs.getString(1);

					// int kindOf = templateRecordRs.getInt(2);

					Long timestamp = templateRecordRs.getLong(3);
					String recordId = templateRecordRs.getString(4);

					// XmlDocumentType doc = (XmlDocumentType)
					// XmlUtil.unmarshall(docXmlStr);
					// doc.setVersionId(recordId);
					// String dd = doc.toJSON();

					BasicDBObject doc1 = new BasicDBObject("id", docId).append("version", recordId).append("type", "TEMPLATE")
							.append("content", docXmlStr).append("timestamp", new Date(timestamp))
							.append("timestamp_l", timestamp);
					ba_xml_coll.insert(doc1);
				} catch (Exception ex)
				{
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

	private static void set__count_references_document() throws Exception
	{
		HashMap<String, Integer> count_references_document = new HashMap<String, Integer>();

		DBCursor cursor = ba_xml_coll.find(new BasicDBObject("type", "DOCUMENT"));

		try
		{
			while (cursor.hasNext())
			{
				DBObject obj = cursor.next();

				String content = (String) obj.get("content");
				String id = (String) obj.get("id");
				String version = (String) obj.get("version");
				XmlDocument doc = (XmlDocument) XmlUtil.unmarshall(content);
				doc.setVersionId(version);

				try
				{
					if (doc.getAuthorId() != null && doc.getAuthorId().length() > 0)
					{
						Integer count_rd = count_references_document.get(doc.getAuthorId());
						if (count_rd == null)
							count_rd = 0;
						count_references_document.put(doc.getAuthorId(), count_rd + 1);
					}

					for (XmlAttribute attr : doc.getAttributes())
					{
						String type = attr.getType();
						String link = null;

						if (type.equals("LINK"))
						{
							link = attr.getLinkValue();
						} else if (type.equals("ORGANIZATION"))
						{
							link = attr.getOrganizationValue();
						} else if (type.equals("DICTIONARY"))
						{
							link = attr.getRecordIdValue();
						}

						if (link != null && link.length() > 0)
						{
							Integer count_rd = count_references_document.get(link);
							if (count_rd == null)
								count_rd = 0;
							count_references_document.put(link, count_rd + 1);
						}
					}

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}
		} finally
		{
			cursor.close();
		}

		// заполним ссчетчики ссылок на документ
		cursor = ba_xml_coll.find(new BasicDBObject("type", "DOCUMENT"));

		try
		{
			while (cursor.hasNext())
			{
				DBObject obj = cursor.next();

				String id = (String) obj.get("id");

				Integer count_rd = count_references_document.get(id);

				if (count_rd != null && count_rd > 0)
				{
					try
					{
						ba_xml_coll.update(new BasicDBObject("id", id), new BasicDBObject("$set", new BasicDBObject(
								"count-references-on-document", count_rd)), true, false);
					} catch (Exception ex)
					{
						ex.printStackTrace();
					}
				}
			}
		} finally
		{
			cursor.close();
		}

	}

	private static String getBAObjOnTimestamp(String id, long timestamp)
	{
		String res = null;
		String queryStr = "SELECT recordId FROM objects WHERE objectId = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1";
		PreparedStatement ps;
		try
		{
			ps = connection.prepareStatement(queryStr);
			ps.setString(1, id);
			ps.setLong(2, timestamp);
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				res = rs.getString(1);
			}
			rs.close();
			ps.close();
		} catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (res == null)
			res = getBAObjOnTimestamp(id, new Date().getTime());

		return res;
	}

	private static int fetchDocuments(long timestamp_start, String doc_id) throws Exception
	{
		String docsIdDataQuery;
		ResultSet docRecordsRs;

		long timestamp_delta = 10 * 24 * 60 * 60 * 1000;

		long prev_doc_count = 0;

		while (timestamp_start < System.currentTimeMillis())
		{
			if (timestamp_start == -1)
				timestamp_start = System.currentTimeMillis();

			if (doc_id != null)
			{
				docsIdDataQuery = "select objectId, recordId, content, timestamp, templateId, templateVersionId "
						+ "FROM objects " + "WHERE isTemplate = 0 and actual = 1 and objectId = '" + doc_id + "'";
			} else
			{
				docsIdDataQuery = "select objectId, recordId, content, timestamp, templateId, templateVersionId "
						+ "FROM objects " + "WHERE isTemplate = 0 and actual = 1 and timestamp > " + timestamp_start
						+ " and timestamp <= " + (timestamp_start + timestamp_delta) + " ORDER BY timestamp ASC";
			}
			docRecordsRs = connection.createStatement().executeQuery(docsIdDataQuery);

			while (docRecordsRs.next())
			{
				String docId = docRecordsRs.getString(1);
				String docXmlStr = docRecordsRs.getString(3);
				String docVersionId = docRecordsRs.getString(2);
				Long timestamp = docRecordsRs.getLong(4);
				String templateId = docRecordsRs.getString(5);
				String templateVersionId = docRecordsRs.getString(6);

				if (templateVersionId == null)
				{
					// найти шаблон на дату и обновить id версии шаблона у данного документа
					templateVersionId = getBAObjOnTimestamp(templateId, timestamp);

					String queryStr = "UPDATE objects SET templateVersionId = ? WHERE objectId = ? AND recordId = ?";
					PreparedStatement ps = connection.prepareStatement(queryStr);
					ps.setString(1, templateVersionId);
					ps.setString(2, docId);
					ps.setString(3, docVersionId);
					ps.execute();
					ps.close();
				}

				BasicDBObject doc1 = new BasicDBObject("id", docId).append("version", docVersionId)
						.append("templateId", templateId).append("templateVersionId", templateVersionId)
						.append("type", "DOCUMENT").append("content", docXmlStr).append("timestamp", new Date(timestamp))
						.append("timestamp_l", timestamp);
				ba_xml_coll.insert(doc1);
				count_documents++;
			}
			docRecordsRs.close();

			timestamp_start += timestamp_delta;

			if (count_documents - prev_doc_count > 0)
				System.out.println("select documents, timestamp [" + timestamp_start + ":" + (timestamp_start + timestamp_delta)
						+ "], count=" + count_documents);

			prev_doc_count = count_documents;
		}

		return count_documents;
	}

	static void connect_to_mysql() throws Exception
	{
		System.out.print("connect to source database " + dbUrl + "...");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl, dbUser, dbPassword);

		System.out.println("ok");
	}

	private static void loadProperties()
	{

		try
		{
			properties.load(new FileInputStream("ba2pacahon.properties"));

			dbUser = properties.getProperty("dbUser", "ba");
			dbPassword = properties.getProperty("dbPassword", "123456");
			dbUrl = properties.getProperty("dbUrl", "localhost:3306");
			host = properties.getProperty("rabbit-host", "localhost");
			port = properties.getProperty("rabbit-port", "5672");
			virtualHost = properties.getProperty("rabbit-virtualHost", "bigarchive");
			userName = properties.getProperty("rabbit-userName", "ba");
			password = properties.getProperty("rabbit-password", "123456");
			queue = properties.getProperty("rabbit-queue", "new-search");
			String mongo = properties.getProperty("mongodb", "localhost:27017");
			String els[] = mongo.split(":");
			mongodb_host = els[0];
			mongodb_port = Integer.parseInt(els[1]);

			String ba_xml_mongo = properties.getProperty("ba-xml-mongodb", "localhost:27017");
			els = ba_xml_mongo.split(":");
			ba_xml_mongodb_host = els[0];
			ba_xml_mongodb_port = Integer.parseInt(els[1]);

			String az_db = properties.getProperty("az-db", "localhost:27017");
			els = az_db.split(":");
			az_mongodb_host = els[0];
			az_mongodb_port = Integer.parseInt(els[1]);
		} catch (IOException e)
		{
			writeDefaultProperties();
		}

	}

	private static void writeDefaultProperties()
	{
		System.out.println("Writing default properties.");

		properties.setProperty("dbUser", "ba");
		properties.setProperty("dbPassword", "123456");
		properties.setProperty("dbUrl", "localhost:3306");
		properties.setProperty("mongodb", "localhost:27017");

		try
		{
			properties.store(new FileOutputStream("ba2pacahon.properties"), null);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private static void toQueueDocumentTypes(BasicDBObject query) throws Exception
	{
		DBCursor cursor = ba_xml_coll.find(query);

		try
		{
			while (cursor.hasNext())
			{
				DBObject obj = cursor.next();
				String content = (String) obj.get("content");
				String version = (String) obj.get("version");
				XmlDocumentType doc = (XmlDocumentType) XmlUtil.unmarshall(content);
				doc.setVersionId(version);

				String dd = doc.toJSON();
				messagingManager.sendMessage(queue, dd);

				// System.out.println(dd);
			}
		} finally
		{
			cursor.close();
		}
	}

	private static void fetchAuthorization() throws Exception
	{
		// это запись о делегировании
		//		String DELEGATION_DELEGATE = (String) obj.get("mo/at/acl#de");
		//		String DELEGATION_OWNER = (String) obj.get("mo/at/acl#ow");
		//		String DELEGATION_WITH_TREE = (String) obj.get("mo/at/acl#wt");
		//		String DELEGATION_DOCUMENT_ID = (String) obj.get("mo/at/acl#dg_doc_id");

		DBCollection az_coll = null;
		MongoClient az_mongoClient = new MongoClient(az_mongodb_host, az_mongodb_port);
		DB az_pacahon_db = az_mongoClient.getDB("az1");
		if (az_mongodb_host != null)
		{
			az_mongoClient = new MongoClient(az_mongodb_host, az_mongodb_port);
			az_pacahon_db = az_mongoClient.getDB("az1");
			az_coll = az_pacahon_db.getCollection("simple");
		}

		MongoClient mongoClient_doc = new MongoClient(mongodb_host, mongodb_port);
		DB pacahon_db_doc = mongoClient_doc.getDB("pacahon");
		simple_coll = pacahon_db_doc.getCollection("simple");

		MongoClient ba_xml_mongoClient = new MongoClient(ba_xml_mongodb_host, ba_xml_mongodb_port);
		DB ba_xml_pacahon_db = ba_xml_mongoClient.getDB("pacahon");
		ba_xml_coll = ba_xml_pacahon_db.getCollection("ba_docs");

		int count_acl = 0;
		int count_doc = 0;

		DBCursor cursor = ba_xml_coll.find(new BasicDBObject("type", "DOCUMENT"));
		while (cursor.hasNext())
		{
			count_doc++;
			DBObject obj = cursor.next();
			String content = (String) obj.get("content");
			String id = (String) obj.get("id");
			String version = (String) obj.get("version");

			DBCursor cursor_az = az_coll.find(new BasicDBObject("mo/at/acl#eId", id));

			while (cursor_az.hasNext())
			{
				count_acl++;
				DBObject obj_az = cursor_az.next();

				// это ACL запись
				String AUTHOR_SUBSYSTEM_ELEMENT = (String) obj_az.get("mo/at/acl#atSsE");
				String TARGET_SUBSYSTEM_ELEMENT = (String) obj_az.get("mo/at/acl#tgSsE");
				String AUTHOR_SUBSYSTEM = (String) obj_az.get("mo/at/acl#atSs");
				String TARGET_SUBSYSTEM = (String) obj_az.get("mo/at/acl#tgSs");
				String DATE_FROM = (String) obj_az.get("mo/at/acl#dtF");
				String DATE_TO = (String) obj_az.get("mo/at/acl#dtT");
				String ELEMENT_ID = (String) obj_az.get("mo/at/acl#eId");
				String RIGHTS = (String) obj_az.get("mo/at/acl#rt");
				String ss = (String) obj_az.get("ss");
				ss = ss.substring(ss.indexOf("#") + 1);

				try
				{
					JSONArray data = new JSONArray();
					JSONObject jo = new JSONObject();
					jo.put("@", "auth:" + ss);

					JSONArray ja = new JSONArray();
					ja.add("docs:Document");
					ja.add("docs:acl");
					jo.put("a", ja);
					if (AUTHOR_SUBSYSTEM.equals("DOCFLOW"))
					{
						DBCursor cursor_1 = simple_coll.find(new BasicDBObject("auth:login", AUTHOR_SUBSYSTEM_ELEMENT));
						if (cursor_1.hasNext())
						{
							DBObject obj_1 = cursor_1.next();
							AUTHOR_SUBSYSTEM_ELEMENT = (String) obj_1.get("@");
						}
						cursor_1.close();
					} else
						AUTHOR_SUBSYSTEM_ELEMENT = "zdb:doc_" + AUTHOR_SUBSYSTEM_ELEMENT;

					jo.put("dc:creator", AUTHOR_SUBSYSTEM_ELEMENT);

					if (TARGET_SUBSYSTEM.equals("DOCFLOW"))
					{
						DBCursor cursor_1 = simple_coll.find(new BasicDBObject("auth:login", TARGET_SUBSYSTEM_ELEMENT));
						if (cursor_1.hasNext())
						{
							DBObject obj_1 = cursor_1.next();
							TARGET_SUBSYSTEM_ELEMENT = (String) obj_1.get("@");
						}
						cursor_1.close();
					} else
						TARGET_SUBSYSTEM_ELEMENT = "zdb:doc_" + TARGET_SUBSYSTEM_ELEMENT;
					jo.put("auth:to", TARGET_SUBSYSTEM_ELEMENT);

					jo.put("auth:target", "zdb:doc_" + ELEMENT_ID);

					ja = new JSONArray();
					for (char cc : RIGHTS.toCharArray())
					{
						ja.add("+" + cc);
					}
					jo.put("auth:rights", ja);

					jo.put("docs:active", "true");
					jo.put("docs:actual", "true");
					jo.put("docs:label", "ACL запись");
					jo.put("dc:identifier", ss);
					data.add(jo);

					put("000-000-000", data, "fetchAuthorization");

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}

				if (count_acl % 1000 == 0)
					System.out.println("count_acl:" + count_acl + ", count_doc:" + count_doc);
			}

			cursor_az.close();
		}

		cursor.close();
		az_mongoClient.close();
		mongoClient_doc.close();
	}

	public static boolean put(String ticket, JSONArray data, String from) throws Exception
	{
		try
		{
			UUID msg_uuid = UUID.randomUUID();

			String args = JSONArray.toJSONString(data);

			String msg = "{\n \"@\" : \"msg:M" + msg_uuid + "\", \n \"a\" : \"msg:Message\",\n" + "\"msg:sender\" : \"" + from
					+ "\",\n \"msg:ticket\" : \"" + ticket
					+ "\", \"msg:reciever\" : \"pacahon\",\n \"msg:command\" : \"put\",\n \"msg:args\" :\n" + args + "\n}";

			// отправляем
			//		socket.send(msg.getBytes(), 0);
			//		byte[] rr = socket.recv(0);
			//		String result = new String(rr, "UTF-8");	

			messagingManager.sendMessage(queue, msg);
			return true;
		} catch (Exception ex)
		{
			return false;
		}
	}

	private static void toQueueDocuments(BasicDBObject query, String is_processed_links) throws Exception
	{
		DBCursor cursor = ba_xml_coll.find(query);

		int count = 0;

		try
		{
			while (cursor.hasNext())
			{
				DBObject obj = cursor.next();

				String content = (String) obj.get("content");
				String version = (String) obj.get("version");
				String typeVersion = (String) obj.get("templateVersionId");

				Object count_references_on_document_o = obj.get("count-references-on-document");

				int count_references_on_document = 0;

				if (count_references_on_document_o != null)
				{
					if (count_references_on_document_o instanceof java.lang.Integer)
					{
						count_references_on_document = (Integer) count_references_on_document_o;
					}
					if (count_references_on_document_o instanceof java.lang.Double)
					{
						count_references_on_document = ((Double) count_references_on_document_o).intValue();
					}

				}

				if ((is_processed_links.equals("N") && count_references_on_document > 0) || is_processed_links.equals("Y"))
				{
					//					if (is_processed_links.equals("Y") && count == 100)
					//						break;

					XmlDocument doc = (XmlDocument) XmlUtil.unmarshall(content);
					doc.setVersionId(version);
					doc.setTypeVersionId(typeVersion);

					try
					{
						JSONObject jo = doc.getJSON();
						jo.put("is-processed-links", is_processed_links);

						if ((is_processed_links.equals("N") && count_references_on_document > 0))
							jo.put("is-cached", "Y");

						String dd = JSONObject.toJSONString(jo);
						messagingManager.sendMessage(queue, dd);

					} catch (Exception ex)
					{
						ex.printStackTrace();
					}

				}
				count++;
			}
		} finally
		{
			cursor.close();
		}
		System.out.println("count docs:" + count);
	}

	public static void main(String[] args) throws Exception
	{
		long start_timestamp = 0;
		byte ADD_DELTA = 0;
		String DOC_ID = null;
		String TEMPLATE_ID = null;

		loadProperties();
		messagingManager = new AMQPMessagingManager();
		messagingManager.init(host, Integer.parseInt(port), virtualHost, userName, password, responseWaitingLimit, null);

		for (int i = 0; i < args.length; i++)
		{
			if (args[i].equals("ADD-DELTA") == true)
			{
				ADD_DELTA = 1;
				if (args.length > 1)
				{
					try
					{
						start_timestamp = Long.parseLong(args[i + 1]);
						System.out.println("param ADD_DELTA, start timestamp=" + start_timestamp);
						i++;
					} catch (Exception ex)
					{

					}
				}

			}
			if (args[i].equals("AZ") == true)
			{
				System.out.println("param AZ");
				fetchAuthorization();
			}
			if (args[i].equals("DOC_ID") == true)
			{
				DOC_ID = args[i + 1];
				System.out.println("param DOC_ID = " + DOC_ID);
			}
			if (args[i].equals("TEMPLATE_ID") == true)
			{
				TEMPLATE_ID = args[i + 1];
				System.out.println("param TEMPLATE_ID = " + TEMPLATE_ID);
			}

		}

		//		MongoClient mongoClient = new MongoClient(mongodb_host, mongodb_port);
		//		DB pacahon_db = mongoClient.getDB("pacahon");

		MongoClient ba_xml_mongoClient = new MongoClient(ba_xml_mongodb_host, ba_xml_mongodb_port);
		DB ba_xml_pacahon_db = ba_xml_mongoClient.getDB("pacahon");
		ba_xml_coll = ba_xml_pacahon_db.getCollection("ba_docs");

		if (ADD_DELTA == 1 && start_timestamp == 0)
		{
			DBCursor cursor = ba_xml_coll.find(new BasicDBObject()).sort(new BasicDBObject("timestamp_l", -1));
			if (cursor.hasNext())
			{
				DBObject obj = cursor.next();

				start_timestamp = (Long) obj.get("timestamp_l");
			}
		}

		try
		{
			if (ba_xml_coll.count() == 0 || ADD_DELTA == 1)
			{
				System.out.println("start sql ba xml -> mongo ba json");
				System.out.println("connect to mysql");
				connect_to_mysql();
				System.out.println("fetchDocumentTypes");
				fetchDocumentTypes(start_timestamp);
				System.out.println("fetchDocuments");
				fetchDocuments(start_timestamp, null);
				if (ADD_DELTA == 0)
				{
					System.out.println("set__count_references_document");
					set__count_references_document();
					System.out.println("end start sql ba xml -> mongo ba json");
				}
			}

			if ((ba_xml_coll.count() > 0 && DOC_ID == null) || ADD_DELTA == 1)
			{
				System.out.println("start ba json -> pacahon");
				System.out.println("TEMPLATE pass I");

				BasicDBObject select_templates = new BasicDBObject("type", "TEMPLATE").append("timestamp_l", new BasicDBObject(
						"$gt", start_timestamp));

				BasicDBObject select_docs = new BasicDBObject("type", "DOCUMENT").append("timestamp_l", new BasicDBObject("$gt",
						start_timestamp));

				if (TEMPLATE_ID != null)
				{
					select_docs.append("templateId", TEMPLATE_ID);
				}

				toQueueDocumentTypes(select_templates);
				System.out.println("TEMPLATE pass II");
				toQueueDocumentTypes(select_templates);
				System.out.println("DOCUMENTS pass I");
				toQueueDocuments(select_docs, "N");
				System.out.println("DOCUMENTS pass II");
				toQueueDocuments(select_docs, "Y");
				System.out.println("end ba json -> pacahon");
			}

			if (ba_xml_coll.count() > 0 && DOC_ID != null)
			{
				connect_to_mysql();				
				System.out.println("start ba json -> pacahon, get document with DOC_ID=" + DOC_ID);
				fetchDocuments(-1, DOC_ID);
				toQueueDocuments(new BasicDBObject("type", "DOCUMENT").append("id", DOC_ID), "Y");
			}

		} catch (Exception ex)
		{
			System.out.println("Error !");
			ex.printStackTrace(System.out);
		}
		//mongoClient.close();
		messagingManager.close();
		System.exit(0);
	}
}
