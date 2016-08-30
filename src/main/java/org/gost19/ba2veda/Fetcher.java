package org.gost19.ba2veda;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;

import org.gost19.pacahon.ba_organization_driver.BaOrganizationDriver;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import ru.mndsc.bigarch.helpers.LocaledValue;
import ru.mndsc.bigarchive.server.kernel.document.XmlUtil;
import ru.mndsc.bigarchive.server.kernel.document.beans.XmlAttribute;
import ru.mndsc.bigarchive.server.kernel.document.beans.XmlDocument;
import ru.mndsc.objects.organization.Department;
import ru.mndsc.objects.organization.User;

import com.bigarchive.filemanager.FileManagerEndpoint;
import com.bigarchive.filemanager.FileManagerService;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Fetcher
{
	private static BaOrganizationDriver pacahon;
	private static Properties properties = new Properties();
	private static Connection documents_db_connection = null;
	private static Connection files_db_connection = null;
	private static String dbUser;
	private static String dbPassword;
	private static String documents_dbUrl;
	private static String files_dbUrl;
	private static String az_mongodb_host;
	private static int az_mongodb_port;
	private static DBCollection az_simple_coll;
	private static String destination;
	private static String vedaTicket;
	private static JSONParser jp;
	private static FileManagerEndpoint filemanager = null;

        private static int put(String jsn, boolean isPrepareEvent) throws InterruptedException
        {       
                int res = 429;
                int count_wait = 0;
                while (res == 429)
                {
                        res = util.excutePut(destination + "/put_individual", "{\"ticket\":\"" + vedaTicket + "\", \"individual\":"
                                        + jsn + ", \"prepare_events\":" + isPrepareEvent + ", \"event_id\":\"\" }");

                        if (res == 429)
                        {
                                Thread.sleep(10);
                                count_wait++;
                        }

                        if (count_wait == 1)
                                System.out.print(".");

                }

                return res;
        }


	private static FileInfo getFileInfo(String id)
	{
		FileInfo fi = null;
		String queryStr = "SELECT name, dateCreated, filePath, fileLength FROM filerecords WHERE id = ?";
		try
		{
			PreparedStatement ps = files_db_connection.prepareStatement(queryStr);
			ps.setString(1, id);
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				fi = new FileInfo();
				fi.name = rs.getString(1);
				fi.date_create = rs.getDate(2);
				fi.file_path = rs.getString(3);
				fi.file_length = rs.getInt(4);
			}
			rs.close();
			ps.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

		return fi;
	}

	private static XmlDocument getDocumentOnTimestamp(String id, long timestamp) throws JAXBException
	{
		XmlDocument doc = null;
		String xml = null;
		String queryStr = "SELECT content FROM objects WHERE objectId = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1";
		try
		{
			PreparedStatement ps = documents_db_connection.prepareStatement(queryStr);
			ps.setString(1, id);
			ps.setLong(2, timestamp);
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				xml = rs.getString(1);
				doc = (XmlDocument) XmlUtil.unmarshall(xml);
			}
			rs.close();
			ps.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

		return doc;
	}

	private static List<String> getBAObjOnTemplateId(String templateId)
	{
		List<String> res = new ArrayList<String>();
		String queryStr = "SELECT DISTINCT objectId FROM objects WHERE templateId = ? AND actual = 1 ORDER BY timestamp DESC";
		try
		{
			PreparedStatement ps = documents_db_connection.prepareStatement(queryStr);
			ps.setString(1, templateId);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				res.add(rs.getString(1));
			}
			rs.close();
			ps.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

		return res;
	}

	static void connect_to_mysql() throws Exception
	{
		Class.forName("com.mysql.jdbc.Driver").newInstance();

		System.out.print("connect to source database " + documents_dbUrl + "...");
		documents_db_connection = DriverManager.getConnection("jdbc:mysql://" + documents_dbUrl, dbUser, dbPassword);
		System.out.print("connect to source database " + files_dbUrl + "...");
		files_db_connection = DriverManager.getConnection("jdbc:mysql://" + files_dbUrl, dbUser, dbPassword);

		System.out.println("ok");
	}

	private static void loadProperties()
	{
		try
		{
			properties.load(new FileInputStream("ba2veda.properties"));

			destination = properties.getProperty("destination", "http://127.0.0.1:8080");

			dbUser = properties.getProperty("dbUser", "ba");
			dbPassword = properties.getProperty("dbPassword", "123456");
			documents_dbUrl = properties.getProperty("documents_dbUrl", "localhost:3306/documents_db");
			files_dbUrl = properties.getProperty("files_dbUrl", "localhost:3306/fm_meta_db");

			String az_db = properties.getProperty("az-db", "localhost:27017");
			String[] els = az_db.split(":");
			az_mongodb_host = els[0];
			az_mongodb_port = Integer.parseInt(els[1]);

			System.out.println(properties);
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
			properties.store(new FileOutputStream("ba2veda.properties"), null);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static BaOrganizationDriver getPacahon() throws Exception
	{
		if (pacahon == null)
		{
			String endpoint = properties.getProperty("pacahon", "tcp://127.0.0.1:5560");
			pacahon = new BaOrganizationDriver(endpoint);
		}
		return pacahon;
	}

        private static String getVedaTicket()
        {
                String res = util.excuteGet(destination
                                + "/authenticate?login=karpovrt&password=a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");

                System.out.println(res);
                try
                {
                        JSONObject oo = (JSONObject) jp.parse(res);

                        return (String) oo.get("id");
                } catch (Exception ex)
                {
                        ex.printStackTrace();
                }
                return null;
        }

	private static JSONArray query(String query)
	{
		String res = util.excuteGet(destination + "/query?ticket=" + vedaTicket + "&query=" + URLEncoder.encode(query));

		System.out.println(res);
		try
		{
			Object oo = jp.parse(res);

			if (oo instanceof JSONArray)
			{
				JSONArray arr = (JSONArray) oo;
				return arr;
			}
			return null;
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	private static void fetchOrganization() throws Exception
	{
		getPacahon();

		List<Department> departments = pacahon.getAllDepartments(false, "ba2veda");

		for (Department department : departments)
		{
			System.out.println(department.getName());

			boolean isActive = !"false".equalsIgnoreCase((String) department.getAttributes().get("active"));

			StringBuffer sb = new StringBuffer();

			util.addProperty(sb, "rdf:type", new Resources().add("v-s:Department", 1));
			util.addProperty(sb, "rdfs:label", new Resources().add(department.getName("ru"), "RU").add(department.getName("en"), "EN"));

			util.addProperty(sb, "v-s:parentUnit", new Resources().add(department.getPreviousId(), 1));
			if (!isActive)
			{
				util.addProperty(sb, "v-s:deleted", new Resources().add("true", 64));
			}
			String jsn = "{\"@\":\"" + department.getId() + "\"," + sb.toString() + "}";
			
			int res = put(jsn, true);
			if (res != 200)
				return;

			List<User> users = pacahon.getUsersByDepartmentId(department.getExtId(), "ru", false, false, "ba2veda");
			for (User user : users)
			{
				String person_id = user.getId();
				String account_id = user.getUid();
				String appointment_id = "ap:" + person_id;
				String FIOD_RU;
				String FIOD_EN;
				String position_id = "a"
						+ new StringBuilder().append(user.getPosition("Ru")).append("").toString().toLowerCase().hashCode() + "-"
						+ new StringBuilder().append(user.getPosition("En")).append("").toString().toLowerCase().hashCode();

				isActive = true;

				System.out.println("\t" + user.getName());

				// //////////////////////////////////////////////////////////////////////////////////////
				sb = new StringBuffer();
				util.addProperty(sb, "rdf:type", new Resources().add("v-s:Person", Resource._Uri));

				if (!isActive)
				{
					util.addProperty(sb, "v-s:deleted", new Resources().add("true", Resource._Bool));
				}
				user.changeLocale("Ru");
				Resource rc1 = null;
				Resource rc2 = null;
				if (user.getName() != null)
					rc1 = new Resource(user.getName(), 2, "RU");
				user.changeLocale("En");
				if (user.getName() != null)
				{
					rc2 = new Resource(user.getName(), 2, "EN");
				}
				if ((rc1 != null) || (rc2 != null))
				{
					util.addProperty(sb, "rdfs:label", new Resources().add(rc1).add(rc2));
				}
				util.addProperty(sb, "v-s:hasAccount", new Resources().add(account_id, 1));
				util.addProperty(sb, "v-s:lastName", new Resources().add(user.getLastName("Ru"), "RU").add(user.getLastName("En"), "EN"));
				FIOD_RU = user.getLastName("Ru");
				FIOD_EN = user.getLastName("En");
				util.addProperty(sb, "v-s:firstName", new Resources().add(user.getFirstName("Ru"), "RU").add(user.getFirstName("En"), "EN"));

				if (user.getFirstName("Ru") != null && user.getFirstName("Ru").length() > 0)
					FIOD_RU += " " + user.getFirstName("Ru").charAt(0) + ".";

				if (user.getFirstName("En") != null && user.getFirstName("En").length() > 0)
					FIOD_EN += " " + user.getFirstName("En").charAt(0) + ".";

				util.addProperty(sb, "v-s:middleName",
						new Resources().add(user.getMiddleName("Ru"), "RU").add(user.getMiddleName("En"), "EN"));

				if (user.getMiddleName("Ru") != null && user.getMiddleName("Ru").length() > 0)
					FIOD_RU += user.getMiddleName("Ru").charAt(0) + ".";

				if (user.getMiddleName("En") != null && user.getMiddleName("En").length() > 0)
					FIOD_EN += user.getMiddleName("En").charAt(0) + ".";

				jsn = "{\"@\":\"" + person_id + "\"," + sb.toString() + "}";
	                        res = put(jsn, true);
				if (res != 200)
				{
					System.out.println("ERR:" + res + "\n" + jsn);
					// return;
				}

				// //////////////////////////////////////////////////////////////////////////////////////
				sb = new StringBuffer();
				util.addProperty(sb, "rdf:type", new Resources().add("v-s:Position", 1));

				util.addProperty(sb, "rdfs:label", new Resources().add(user.getPosition("Ru"), "RU").add(user.getPosition("En"), "EN"));

				FIOD_RU += " " + user.getPosition("Ru");
				FIOD_EN += " " + user.getPosition("En");

				jsn = "{\"@\":\"" + position_id + "\"," + sb.toString() + "}";
	                        res = put(jsn, true);
				if (res != 200)
				{
					System.out.println("ERR:" + res + "\n" + jsn);
					// return;
				}

				// //////////////////////////////////////////////////////////////////////////////////////
				sb = new StringBuffer();
				util.addProperty(sb, "rdf:type", new Resources().add("v-s:Appointment", 1));
				if (!isActive)
				{
					util.addProperty(sb, "v-s:deleted", new Resources().add("true", 64));
				}
				util.addProperty(sb, "rdfs:label", new Resources().add(FIOD_RU, "RU").add(FIOD_EN, "EN"));
				util.addProperty(sb, "v-s:occupation", new Resources().add(position_id, 1));
				util.addProperty(sb, "v-s:employee", new Resources().add(user.getId(), 1));

				jsn = "{\"@\":\"" + appointment_id + "\"," + sb.toString() + "}";
	                        res = put(jsn, true);
				if (res != 200)
				{
					System.out.println("ERR:" + res + "\n" + jsn);
					// return;
				}

				// //////////////////////////////////////////////////////////////////////////////////////
				sb = new StringBuffer();
				util.addProperty(sb, "rdf:type", new Resources().add("v-s:Account", 1));
				if (!isActive)
				{
					util.addProperty(sb, "v-s:deleted", new Resources().add("true", 64));
				}
				util.addProperty(sb, "v-s:login", new Resources().add(user.getLogin(), 2));
				util.addProperty(sb, "v-s:mailbox", new Resources().add(user.getEmail(), 2));
				util.addProperty(sb, "v-s:owner", new Resources().add(person_id, 1));

				jsn = "{\"@\":\"" + account_id + "\"," + sb.toString() + "}";
	                        res = put(jsn, true);
				if (res != 200)
				{
					System.out.println("ERR:" + res + "\n" + jsn);
					// return;
				}

			}
		}

	}

	private static void fetchAuthorization() throws Exception
	{
		MongoClient az_mongoClient = new MongoClient(az_mongodb_host, az_mongodb_port);
		DB az_pacahon_db = az_mongoClient.getDB("az1");
		if (az_mongodb_host != null)
		{
			az_mongoClient = new MongoClient(az_mongodb_host, az_mongodb_port);
			az_pacahon_db = az_mongoClient.getDB("az1");
			az_simple_coll = az_pacahon_db.getCollection("simple");
		}

		az_mongoClient.close();
	}

	public static HashMap<String, Ba2VedaParam> ba2veda_map;
	public static HashMap<String, String> prepared_ids;
	public static HashMap<String, String> src_list;

	static int count;

	private static boolean prepare_document(String docId, String path) throws Exception
	{
		if (prepared_ids.get(docId) != null)
			return false;
		prepared_ids.put(docId, "Y");

		if (path.indexOf(docId) > 0)
			return false;

		XmlDocument doc = getDocumentOnTimestamp(docId, new Date().getTime());
		if (doc == null)
			return false;

		count++;
		System.out.println(count + " " + docId);

		StringBuffer sb = new StringBuffer();

		String templateId = doc.getTypeId();
		Ba2VedaParam veda_class = ba2veda_map.get(templateId);

		if (veda_class == null)
		{
			System.out.println("WARN:onto not found : " + templateId);
			return false;
		}

		util.addProperty(sb, "rdf:type", new Resources().add(veda_class.veda_name, Resource._Uri));
		//util.addProperty(sb, "rdfs:label", new Resources().add(doc.getId(), "RU").add(doc.getId(), "EN"));

		util.addProperty(sb, "v-s:author", new Resources().add("ap:" + doc.getAuthorId(), Resource._Uri));
		Date date_created = doc.getDateCreated();

		List<XmlAttribute> atts = doc.getAttributes();
		HashMap<String, Resources> field_records = new HashMap<String, Resources>();

		for (XmlAttribute att : atts)
		{
			String code = att.getCode();
			String type = att.getType();

			Ba2VedaParam map_param = ba2veda_map.get(templateId + code);

			if (map_param != null)
			{
				Resources records = field_records.get(map_param.veda_name);
				if (records == null)
				{
					records = new Resources();
					field_records.put(map_param.veda_name, records);
				}

				if (type.equals("TEXT"))
				{
					String str = att.getTextValue();

					if (str != null && str.length() > 0)
					{
						if (str.indexOf("@@") > 0)
						{
							LocaledValue lv;
							String ru = null;
							String en = null;
							try
							{
								lv = new LocaledValue(str);

								ru = lv.get("ru");
								en = lv.get("en");
							} catch (java.lang.Exception ex)
							{
								lv = new LocaledValue(str);
								ex = ex;
							}

							if (ru != null && ru.length() > 0)
								records.add(ru, "RU");

							if (en != null && en.length() > 0)
								records.add(en, "EN");
						} else
							records.add(str, "NONE");
					}

				} else if (type.equals("DATE"))
				{
					Date date = att.getDateValue();
					if (date != null)
					{
						records.add(date);
					}
				} else if (type.equals("ORGANIZATION"))
				{
					String otype = att.getOrganizationTag();
					String link = att.getOrganizationValue();
					if (link != null && link.length() > 3)
					{
						if (otype.equals("department"))
							records.add(link, Resource._Uri);
						else
							records.add("ap:" + link, Resource._Uri);
					}
				} else if (type.equals("DICTIONARY"))
				{
					String link = att.getRecordIdValue();
					if (link != null && link.length() > 3)
					{
						prepare_document(link, path + docId);
						records.add(link, Resource._Uri);
					}
				} else if (type.equals("LINK"))
				{
					String link = att.getLinkValue();
					if (link != null && link.length() > 3)
					{
						prepare_document(link, path + docId);
						records.add(link, Resource._Uri);
					}

				} else if (type.equals("NUMBER"))
				{
					String val = att.getNumberValue();
					if (val != null && val.length() > 0)
					{
						try
						{
							Double.parseDouble(val);
							records.add(val, Resource._Decimal);
						} catch (Exception ex)
						{
							records.add(val, Resource._String);
						}
					}
				} else if (type.equals("FILE"))
				{
					String fileId = att.getFileValue();
					FileInfo fi = getFileInfo(fileId);

					if (fi != null && date_created != null)
					{
						String path_to_files = util.date2_short_string(date_created).replace('-', '/');
						String file_uri = fileId;

						// создадим описание к загруженному файлу
						StringBuffer sbf = new StringBuffer();
						util.addProperty(sbf, "rdf:type", new Resources().add("v-s:File", Resource._Uri));
						util.addProperty(sbf, "rdfs:label", new Resources().add(fi.name, Resource._String));
						util.addProperty(sbf, "v-s:fileName", new Resources().add(fi.name, Resource._String));
						util.addProperty(sbf, "v-s:fileSize", new Resources().add(fi.file_length + "", Resource._String));
						util.addProperty(sbf, "v-s:fileURI", new Resources().add(path_to_files + "/" + file_uri, Resource._String));
						util.addProperty(sbf, "v-s:author", new Resources().add("ap:" + doc.getAuthorId(), Resource._Uri));
						String jsn = "{\"@\":\"" + file_uri + "\"," + sbf.toString() + "}";
			                        int res = put(jsn, true);
						if (res != 200)
							return false;

						String link = file_uri;
						if (link != null && link.length() > 3)
						{
							records.add(link, Resource._Uri);
						}
					}
					/*
					{				
						Collection<String> ids = new ArrayList<String>();
						ids.add(fileId);

						GetFilesRequestEnvelope request = new GetFilesRequestEnvelope();
						request.setHeader(new RequestHeaderType());
						request.getHeader().setContextName("big-archive");
						request.setWithoutData(false);
						Ids idsO = new Ids();
						idsO.getId().addAll(ids);
						request.setIds(idsO);

						GetFilesResponseEnvelope files = null;
						try
						{
							files = filemanager.getFiles(request);
							FileEntries fileEntries = files.getFileEntries();
							for (FileEntryType fe : fileEntries.getFileEntry())
							{
								DataHandler dh = fe.getData();
								InputStream is = dh.getInputStream();

								String file_uri = fileId;

								//								if (util.upload(destination + "/upload", is, file_uri, path_to_files) == 0)
								{
									// создадим описание к загруженному файлу
									StringBuffer sbf = new StringBuffer();
									util.addProperty(sbf, "rdf:type", new Resources().add("v-s:File", Resource._Uri));
									util.addProperty(sbf, "rdfs:label", new Resources().add(fe.getName(), Resource._String));
									util.addProperty(sbf, "v-s:fileName", new Resources().add(fe.getName(), Resource._String));
									util.addProperty(sbf, "v-s:fileSize", new Resources().add(fe.getSize() + "", Resource._String));
									util.addProperty(sbf, "v-s:fileURI",
											new Resources().add(path_to_files + "/" + file_uri, Resource._String));
									util.addProperty(sbf, "v-s:author", new Resources().add("ap:" + doc.getAuthorId(), Resource._Uri));
									String jsn = "{\"@\":\"" + file_uri + "\"," + sbf.toString() + "}";
									int res = util.excutePut(destination + "/put_individual", "{\"ticket\":\"" + vedaTicket
											+ "\", \"individual\":" + jsn + "}");

									if (res != 200)
										return false;

									String link = file_uri;
									if (link != null && link.length() > 3)
									{
										records.add(link, Resource._Uri);
									}

								}
							}

						} catch (GetFilesFault e)
						{
							throw new FileManagerServerException("Exception on server-side while getting files: " + e.getMessage(), e);
						}
					}
					*/

				}
			}
		}

		Set<Entry<String, Resources>> entries = field_records.entrySet();

		for (Entry<String, Resources> entry : entries)
		{
			String predicate = entry.getKey();
			Resources rcs = entry.getValue();

			if (rcs.resources.size() > 0)
			{
				util.addProperty(sb, predicate, rcs);
			}
		}

		String jsn = "{\"@\":\"" + doc.getId() + "\"," + sb.toString() + "}";
                int res = put(jsn, true);
		if (res != 200)
		{
			//res = util.excutePut(destination + "/put_individual", "{\"ticket\":\"" + vedaTicket + "\", \"individual\":" + jsn + "}");
			return false;
		}

		return true;
	}

	public static JSONObject getIndividual(String uri)
	{
		String res = util.excuteGet(destination + "/get_individual?ticket=" + vedaTicket + "&uri=" + uri);

		System.out.println(res);
		try
		{
			JSONObject oo = (JSONObject) jp.parse(res);

			return oo;
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	private static String getValue(JSONObject doc, String field_name)
	{
		String res = null;

		JSONArray code_obj = (JSONArray) doc.get(field_name);
		if (code_obj != null)
		{
			String code = (String) ((JSONObject) (code_obj.get(0))).get("data");
			if (code != null)
			{
				return code;
			}
		}
		return res;
	}

	public static void map_init()
	{
		prepared_ids = new HashMap<String, String>();
		ba2veda_map = new HashMap<String, Ba2VedaParam>();
		src_list = new HashMap<String, String>();

		JSONArray res = query("'rdf:type' == 'v-s:Transform'");
		if (res != null && res.size() > 0)
		{
			for (int i = 0; i < res.size(); i++)
			{
				String map_record_id = (String) res.get(i);
				JSONObject mapper = getIndividual(map_record_id);

				String deprecatedClass = getValue(mapper, "v-s:srcClassName");
				src_list.put(deprecatedClass, "Y");
				String deprecatedProperty = getValue(mapper, "v-s:srcPropertyName");
				String destProperty = getValue(mapper, "v-s:destProperty");
				String destClass = getValue(mapper, "v-s:destClass");

				if (ba2veda_map.get(deprecatedClass) == null)
					ba2veda_map.put(deprecatedClass, new Ba2VedaParam(destClass));

				ba2veda_map.put(deprecatedClass + deprecatedProperty, new Ba2VedaParam(destProperty));
			}
		}

	}

	public static void prepare_documents_of_type(String templateId) throws Exception
	{
		System.out.println("prepare_documents_of_type: " + templateId);
		List<String> docIds = getBAObjOnTemplateId(templateId);

		for (String docId : docIds)
		{
			prepare_document(docId, "");
		}
	}

	public static FileManagerEndpoint getAttachmentPort()
	{
		if (filemanager == null)
		{
			try
			{
				URL url = new URL("file:wsdl/filemanager.wsdl");
				QName qname = new QName("http://filemanager.zdms_component.mndsc.ru/", "FileManagerService");

				filemanager = new FileManagerService(url, qname).getFileManagerEndpointPort();
			} catch (Throwable e)
			{
				throw new IllegalStateException("Ошибка инициализации соединения с сервисом атачментов", e);
			}

		}

		return filemanager;
	}

	public static void main(String[] args) throws Exception
	{
		jp = new JSONParser();

		loadProperties();
		connect_to_mysql();

		vedaTicket = getVedaTicket();

		if ((vedaTicket == null) || (vedaTicket.length() < 1))
		{
			System.out.println("Login filed");
			return;
		}

		map_init();

//		getAttachmentPort();
//		fetchOrganization();

		for (String type : src_list.keySet())
		{
			vedaTicket = getVedaTicket();

			if (args.length > 0)
			{
			    for (String arg : args)
			    {
				if (arg.equals (type))			    
				    prepare_documents_of_type(type);
			    }	
			}
			else
			    prepare_documents_of_type(type);
		}

		System.out.println("complete");
		System.exit(0);
	}
}