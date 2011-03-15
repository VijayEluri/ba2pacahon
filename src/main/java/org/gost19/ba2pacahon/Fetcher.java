package org.gost19.ba2pacahon;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import magnetico.objects.organization.Department;
import magnetico.ws.organization.AttributeType;
import magnetico.ws.organization.EntityType;
import net.n3.nanoxml.IXMLElement;
import net.n3.nanoxml.IXMLParser;
import net.n3.nanoxml.IXMLReader;
import net.n3.nanoxml.StdXMLReader;
import net.n3.nanoxml.XMLParserFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class Fetcher
{

	private static String documentTypeId;
	private static String ticketId;
	private static String SEARCH_URL;
	// private static String DOCUMENT_SERVICE_URL;
	private static String pathToDump;
	private static Properties properties = new Properties();
	// private static ArrayList<String> roles = new ArrayList<String>();
	// private static ArrayList<String> admins = new ArrayList<String>();
	private static Connection connection = null;
	private static String dbUser;
	private static String dbPassword;
	private static String dbUrl;
	private static String dbSuffix;
	private static HashMap<String, String> code_onto = new HashMap<String, String>();
	private static HashMap<String, String> old_code__new_code = new HashMap<String, String>();
	private static String exclude_codes = "$comment, $private, $authorId, $defaultRepresentation";
	private static HashMap<String, EntityType> userUri__userObj = new HashMap<String, EntityType>();
	private static OrganizationUtil organizationUtil;
	// private static String user_docflow =
	// "541e2793-abc4-437a-9c71-6a1ac0434acf";

	private static HashMap<String, String> group__Id_name;

	public static void main(String[] args) throws Exception
	{
		group__Id_name = new HashMap<String, String>();
		// это не организации и не подразделения, это группы:
		group__Id_name.put("fb6583b7-ed14-492f-bf92-45eb3cab3a56", "СТЕП_подрядные организации");
		group__Id_name.put("bbee1427-9362-4a8b-8056-4ba4b772a5a0", "Другие_Others");
		group__Id_name.put("926abd62-c00d-4f9e-8e36-dcb76aa0c23a", "_Проект'STEP'");
		group__Id_name.put("b0569cc4-4cd5-4aab-827b-4bcc61371ab9", "Проект 'Новый Взгляд'");
		group__Id_name.put("3ac5dbf1-9281-4f5e-93a5-b41657223ce6", "Холдинг");
		group__Id_name.put("5bf316d8-2424-4780-a81b-262214543f61", "Филиалы ООО \"Финлеском\"");
		group__Id_name.put("09fb4c26-b7f6-441d-9924-deda3685bb9d", "Magnetico");
		group__Id_name.put("dbf04a9b-994c-44a7-9153-8341982f8390", "Сторонние организации");
		group__Id_name.put("0056f4df-e72c-4c4a-82cb-a1ac752a615e", "Структура");

		// exclude_code.put("$parentDocumentId", "Y");
		{
			code_onto.put("date_from", predicates.swrc__startDate);
			code_onto.put("to", predicates.swrc__endDate);
			code_onto.put("date_to", predicates.swrc__endDate);
			code_onto.put("Дата окончания (планируемая)", predicates.swrc__endDate);
			code_onto.put("from", predicates.docs__from);
			code_onto.put("От кого", predicates.docs__from);
			code_onto.put("Кому", predicates.docs__to);
			code_onto.put("name", predicates.swrc__name);
			code_onto.put("Name", predicates.swrc__name);
			code_onto.put("Название", predicates.swrc__name);
			code_onto.put("Наименование", predicates.swrc__name);
			code_onto.put("name", predicates.swrc__name);
			code_onto.put("Полное название", predicates.swrc__name);
			code_onto.put("Тема", predicates.dc__subject);
			code_onto.put("Заголовок", predicates.dc__title);
			code_onto.put("Тип", predicates.dc__type);
			code_onto.put("Подразделение", predicates.docs__unit);
			code_onto.put("Дата окончания", predicates.swrc__endDate);
			code_onto.put("Дата начала", predicates.swrc__startDate);
			code_onto.put("В копию", predicates.docs__carbon_copy);
			code_onto.put("Контрагент (название/ страна/ город)", predicates.docs__contractor);
			code_onto.put("Контрагент", predicates.docs__contractor);
			code_onto.put("Вложения", predicates.docs__FileDescription);
			code_onto.put("Вложение", predicates.docs__FileDescription);
			code_onto.put("file", predicates.docs__FileDescription);
			code_onto.put("Ключевые слова", predicates.swrc__keywords);
			code_onto.put("Номер", predicates.swrc__number);
			code_onto.put("Ссылка на документ", predicates.docs__link);
			code_onto.put("Содержание", predicates.docs__content);
			code_onto.put("Краткое содержание", predicates.dc__description);
			code_onto.put("Комментарии", predicates.swrc__note);
			code_onto.put("Комментарий", predicates.swrc__note);

			/*
			 * [Связанные документы]:19 [Цех]:15 [Дата регистрации]:13
			 * [status]:13 [Разработчик]:12 [Объект ТОРО]:11 [Конструкторская
			 * заявка]:10 [Раздел]:10 [Регистрационный номер]:9 [Тип работ]:9
			 * [status_history]:9 [Подписывающий]:9 [Проект]:8 [Дата старта
			 * маршрута]:8 [Дата получения]:8 [Количество листов]:8 [Инв.№]:8
			 * [Обозначение]:8 [Инициатор]:7 [Лист]:6 [Шифр]:6 [Исполнитель]:5
			 * [Вид документа]:5 [Регистрационный номер 1]:5 [Код]:4
			 * [Ответственное лицо]:4 [Список рассылки]:4 [Сопровождающие
			 * документы]:3 [Руководитель проекта]:3 [Источник финансирования]:3
			 * [Конструкторский проект]:3 [Предмет договора]:3 [Дата
			 * заключения]:3 [Объект]:3
			 */
		}

		loadProperties();
		init_source();

		organizationUtil = new OrganizationUtil(properties.getProperty("organizationUrl"),
				properties.getProperty("organizationNameSpace"), properties.getProperty("organizationName"));

		PacahonClient pacahon_client = new PacahonClient(null);
		String ticket = pacahon_client.get_ticket("user", "9cXsvbvu8=");

		// fetchOrganization(pacahon_client, ticket);
		fetchDocumentTypes(pacahon_client, ticket);
		// walkOnDocuments(pacahon_client, ticket);

	}

	/**
	 * Выгружает данные структуры документов в виде пользовательских онтологий
	 */

	private static void fetchDocumentTypes(PacahonClient pacahon_client, String ticket) throws Exception
	{
		IXMLParser parser = XMLParserFactory.createDefaultXMLParser();

		HashMap<String, Integer> code_stat = new HashMap<String, Integer>();

		Model node = ModelFactory.createDefaultModel();
		node.setNsPrefixes(predicates.getPrefixs());

		Resource r = null;

		// writeTriplet(predicates.f_user_onto, predicates.owl__imports,
		// predicates.dc, false);
		// writeTriplet(predicates.f_user_onto, predicates.owl__imports,
		// predicates.f_swrc, false);
		// writeTriplet(predicates.f_user_onto, predicates.owl__imports,
		// predicates.gost19, false);
		// writeTriplet(predicates.f_user_onto, predicates.owl__imports,
		// predicates.docs, false);
		// writeTriplet(predicates.f_user_onto, predicates.owl__imports,
		// predicates.user_onto, false);
		// writeTriplet(predicates.f_user_onto, predicates.rdf__type,
		// predicates.owl__Ontology, false);

		String docsIdDataQuery = "select objectId FROM objects where isTemplate = 1 and timestamp is null";
		ResultSet docRecordsRs = connection.createStatement().executeQuery(docsIdDataQuery);

		while (docRecordsRs.next())
		{
			String docId = docRecordsRs.getString(1);

			String docDataQuery = "select distinct content FROM objects where objectId = '" + docId
					+ "' order by timestamp desc";
			Statement st1 = connection.createStatement();
			ResultSet docRecordRs = st1.executeQuery(docDataQuery);

			int count_of_version = 0;

			while (docRecordRs.next())
			{
				count_of_version++;
				String docXmlStr = docRecordRs.getString(1);

				IXMLReader reader = StdXMLReader.stringReader(docXmlStr);
				parser.setReader(reader);
				IXMLElement xmlDoc = (IXMLElement) parser.parse(true);
				reader.close();

				String authorId = get(xmlDoc, "authorId", null);
				String dateCreated = get(xmlDoc, "dateCreated", null);
				String lastModifiedTime = get(xmlDoc, "dateLastModified", null);
				//				String lastEditorId = get(xmlDoc, "lastEditorId", null);
				//				String objectType = get(xmlDoc, "objectType", null);
				//				String typeId = get(xmlDoc, "typeId", null);

				String name = get(xmlDoc, "name", null);

				// XMLGregorianCalendar dateCreated = documentTypeType.getDateCreated();
				// XMLGregorianCalendar lastModifiedTime = documentTypeType.getLastModifiedTime();
				// String name = documentTypeType.getName();
				// String systemInformation = documentTypeType.getSystemInformation();

				if (dateCreated != null && lastModifiedTime != null)
				{
					if (dateCreated.equals(lastModifiedTime))
					{
						System.out.println("dateCreated == lastModifiedTime");
					} else
					{
						System.out.println("dateCreated != lastModifiedTime");
					}
				}

				String activeStatusLabel = "";
				if (dateCreated == null)
					activeStatusLabel = "удален";

				String draftStatusLabel = "";
				// if (documentTypeType.isInDraftState() == true)
				// draftStatusLabel = "черновик";

				// System.out.println(String.format("\n%s:[%s] %s %s",
				// documentTypeType.getId(), name, activeStatusLabel,
				// draftStatusLabel));

				// String[] si_elements = null;

				// if (systemInformation != null)
				// si_elements = systemInformation.split(";");

				String id = get(xmlDoc, "id", null);

				r = node.createResource(predicates.user_onto + "template_" + id + "_v_" + count_of_version);

				r.addProperty(ResourceFactory.createProperty(predicates.dc__identifier), node.createLiteral(id));

				r.addProperty(ResourceFactory.createProperty(predicates.rdfs__subClassOf),
						ResourceFactory.createProperty(predicates.docs__Document));

				r.addProperty(ResourceFactory.createProperty(predicates.rdf, "type"),
						ResourceFactory.createProperty(predicates.rdfs__Class));

				r.addProperty(ResourceFactory.createProperty(predicates.dc, "creator"),
						ResourceFactory.createProperty(predicates.zdb, "person_" + authorId));

				if (dateCreated != null)
					r.addProperty(ResourceFactory.createProperty(predicates.swrc__creationDate),
							node.createLiteral(dateCreated.toString()));

				if (lastModifiedTime != null)
					r.addProperty(ResourceFactory.createProperty(predicates.dc__date),
							node.createLiteral(lastModifiedTime.toString()));

				LangString lName = LangString.parse(name);

				if (lName.text_ru != null)
					r.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
							node.createLiteral(lName.text_ru, "ru"));

				if (lName.text_en != null)
					r.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
							node.createLiteral(lName.text_en, "en"));

				// /////////////////////////////////////////////////////////////////////////////////////////////
				Vector<IXMLElement> atts = null;

				atts = xmlDoc.getFirstChildNamed("xmlAttributes").getChildren();

				int ii = 0;
				if (atts != null)
				{
					for (IXMLElement att_list_element : atts)
					{
						ii++;

						String att_name = get(att_list_element, "name", "");

						if (exclude_codes.indexOf(att_name) >= 0)
						{
							System.out.println("\n	att_name=[" + att_name + "] is skipped");

							continue;
						}

						String restrictionId = "template_" + id + "_f_" + ii;

						System.out.println("\n");

						Resource rr = node.createResource(predicates.user_onto + restrictionId);
						rr.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
								ResourceFactory.createProperty(predicates.owl__Restriction));

						r.addProperty(ResourceFactory.createProperty(predicates.rdfs__subClassOf),
								node.createProperty(predicates.user_onto, restrictionId));

						lName = LangString.parse(att_name);

						if (lName.text_ru != null)
							rr.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
									node.createLiteral(lName.text_ru, "ru"));

						if (lName.text_en != null)
							rr.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
									node.createLiteral(lName.text_en, "en"));

						String code = get(att_list_element, "code", null);

						System.out.println("	code=[" + code + "]");

						// подсчет частоты встречаемости code
						Integer count = code_stat.get(code);
						if (count != null)
							count = new Integer(count.intValue() + 1);
						else
						{
							count = new Integer(1);
						}

						// переименование code в onto
						String this_code_in_onto = renameCodeToOnto(code, id, lName.text_ru);

						rr.addProperty(ResourceFactory.createProperty(predicates.owl__onProperty),
								node.createProperty(this_code_in_onto));

						code_stat.put(code, count);

						String descr = get(att_list_element, "description", "");

						String multi_select_label = "";
						String obligatory_label = "";

						obligatory_label = get(att_list_element, "obligatory", "false");
						multi_select_label = get(att_list_element, "multiSelect", "false");

						if (multi_select_label.equals("true"))
						{
							multi_select_label = "[множественное]";
						} else
						{
							// ? <http://www.w3.org/2002/07/owl#maxCardinality>
							// "1"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>
							// .
							rr.addProperty(ResourceFactory.createProperty(predicates.owl__maxCardinality),
									node.createLiteral("1"));
						}

						if (obligatory_label.equals("true"))
						{
							obligatory_label = "[обязательное]";

							// <> <http://www.w3.org/2002/07/owl#minCardinality>
							// "1"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>
							// .
							rr.addProperty(ResourceFactory.createProperty(predicates.owl__minCardinality),
									node.createLiteral("1"));
						} else
						{
							// ?
						}

						// att_list_element.getDateCreated();

						String type = get(att_list_element, "type", "");

						String typeLabel = null;

						String obj_owl__allValuesFrom = null;

						if (type.equals("BOOLEAN"))
						{
							typeLabel = "boolean";
							obj_owl__allValuesFrom = predicates.xsd__boolean;
						} else if (type.equals("DATE"))
						{
							typeLabel = "date";
							obj_owl__allValuesFrom = predicates.xsd__date;
						} else if (type.equals("DATEINTERVAL"))
						{
							typeLabel = "date_interval";
							obj_owl__allValuesFrom = predicates.docs__dateInterval;
						} else if (type.equals("FILE"))
						{
							obj_owl__allValuesFrom = predicates.docs__FileDescription;
							typeLabel = "attachment";
						} else if (type.equals("LINK") || type.equals("DICTIONARY"))
						{
							if (type.equals("DICTIONARY"))
							{
								String dictionaryIdValue = get(att_list_element, "dictionaryIdValue", null);

								if (dictionaryIdValue != null)
								{
									System.out.println("dictionaryIdValue = " + dictionaryIdValue);
									obj_owl__allValuesFrom = predicates.user_onto + dictionaryIdValue;
								}

							} else
								obj_owl__allValuesFrom = predicates.docs__Document;

							if (descr.indexOf("$composition") >= 0)
							{
								// нужно вынести в hasValue импортируемые поля
								// из ссылаемого документа
								String[] compzes = descr.split(";");

								for (String compz : compzes)
								{
									if (compz.indexOf("$isTable") >= 0)
									{
										String[] tmpa = compz.split("=");
										if (tmpa.length > 1)
										{
											String data = tmpa[1];

											obj_owl__allValuesFrom = predicates.user_onto + id;
										}
									}
									if (compz.indexOf("$composition") >= 0)
									{
										String[] tmpa = compz.split("=");
										if (tmpa.length > 1)
										{
											String data = tmpa[1];
											if (data != null)
											{
												String[] importsFields = data.replace('|', ';').split(";");

												for (String field : importsFields)
												{
													String new_code = old_code__new_code.get(field);

													if (new_code == null)
														new_code = renameCodeToOnto(field, null, null);

													rr.addProperty(
															ResourceFactory.createProperty(predicates.owl__hasValue),
															node.createLiteral(new_code));

													new_code += "";
												}
											}
										}

									}
								}

							}
							if (type.equals("LINK"))
								typeLabel = "document_link";
							else
								typeLabel = "dictionary";

						} else if (type.equals("NUMBER"))
						{
							typeLabel = "number";
							obj_owl__allValuesFrom = predicates.xsd__integer;
						} else if (type.equals("ORGANIZATION"))
						{
							typeLabel = "organization";
							// obj_owl__allValuesFrom =
							// Predicate.swrc__Organization;

							// нужно определить что это за тип, person ?
							// department ? organization
							String organizationTag = get(att_list_element, "organizationTag", "");

							String[] organizationTags = organizationTag.split(";");

							for (String tag : organizationTags)
							{
								if (tag.equals("user"))
								{
									// для этого типа нужно добавить:

									rr.addProperty(ResourceFactory.createProperty(predicates.owl__hasValue),
											ResourceFactory.createProperty(predicates.swrc__lastName));

									rr.addProperty(ResourceFactory.createProperty(predicates.owl__hasValue),
											ResourceFactory.createProperty(predicates.swrc__firstName));

									if (organizationTags.length == 1)
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__allValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Person));
									else
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__someValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Person));

								} else if (tag.equals("department"))
								{
									rr.addProperty(ResourceFactory.createProperty(predicates.owl__hasValue),
											ResourceFactory.createProperty(predicates.swrc__name));

									if (organizationTags.length == 1)
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__allValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Department));
									else
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__someValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Department));
								}
							}

						} else if (type.equals("TEXT"))
						{
							typeLabel = "text";
							rr.addProperty(ResourceFactory.createProperty(predicates.owl__allValuesFrom),
									ResourceFactory.createProperty(predicates.xsd__string));
						}

						System.out.println("	att_name=[" + att_name + "]:" + typeLabel + " " + obligatory_label + " "
								+ multi_select_label);
						System.out.println("	description=[" + descr + "]");

					}
					pacahon_client.put(ticket, node);
				}

			}
			docRecordRs.close();
			st1.close();

		}

		System.out.println("\ncode entry stat");
		for (int ii = 20; ii > 0; ii--)
		{
			for (Entry<String, Integer> ee : code_stat.entrySet())
			{
				if (ee.getValue() == ii)
					System.out.println("[" + ee.getKey() + "]:" + ee.getValue());
			}
		}

		System.out.println("\ncode -> onto");
		for (Entry<String, String> ee : old_code__new_code.entrySet())
		{
			System.out.println("[" + ee.getKey() + "]->[" + ee.getValue() + "]");
		}

	}

	private static int walkOnDocuments(PacahonClient pacahon_client, String ticket) throws Exception
	{
		int count_documents = 0;

		IXMLParser parser = XMLParserFactory.createDefaultXMLParser();

		// writeTriplet(predicates.f_zdb, predicates.owl__imports,
		// predicates.docs, false);
		// writeTriplet(predicates.f_zdb, predicates.owl__imports,
		// predicates.f_swrc, false);
		// writeTriplet(predicates.f_zdb, predicates.owl__imports,
		// predicates.gost19, false);
		// writeTriplet(predicates.f_zdb, predicates.rdf__type,
		// predicates.owl__Ontology, false);

		String docsIdDataQuery = "select objectId FROM objects where isTemplate = 0 and timestamp is null";
		ResultSet docRecordsRs = connection.createStatement().executeQuery(docsIdDataQuery);

		while (docRecordsRs.next())
		{
			String docId = docRecordsRs.getString(1);

			String docDataQuery = "select distinct content FROM objects where objectId = '" + docId
					+ "' order by timestamp asc";
			Statement st1 = connection.createStatement();
			ResultSet docRecordRs = st1.executeQuery(docDataQuery);

			while (docRecordRs.next())
			{
				String id = null;
				try
				{
					count_documents++;

					if (count_documents % 1000 == 0)
						System.out.println("count prepared documents = " + count_documents);

					String docXmlStr = docRecordRs.getString(1);

					IXMLReader reader = StdXMLReader.stringReader(docXmlStr);
					parser.setReader(reader);
					IXMLElement xmlDoc = (IXMLElement) parser.parse(true);
					reader.close();

					String authorId = get(xmlDoc, "authorId", null);
					String dateCreated = get(xmlDoc, "dateCreated", null);
					String dateLastModified = get(xmlDoc, "dateLastModified", null);
					String lastEditorId = get(xmlDoc, "lastEditorId", null);
					String objectType = get(xmlDoc, "objectType", null);
					String typeId = get(xmlDoc, "typeId", null);
					id = get(xmlDoc, "id", null);

					String newDocSubj = predicates.zdb + id;

					addPersonToDocument(newDocSubj, authorId, predicates.dc__creator);

					// writeTriplet(newDocSubj, predicates.rdf__type,
					// predicates.user_onto + typeId, false);

					Vector<IXMLElement> atts = null;
					atts = xmlDoc.getFirstChildNamed("xmlAttributes").getChildren();

					if (atts != null)
					{
						for (IXMLElement att_list_element : atts)
						{
							String type = get(att_list_element, "type", "");
							String att_code = get(att_list_element, "code", "");
							String onto_code = old_code__new_code.get(att_code);
							if (onto_code == null)
								continue;

							if (type.equals("ORGANIZATION"))
							{
								/*
								 * <xmlAttribute>
								 * <dateCreated>2010-02-02T09:40:08.862
								 * +03:00</dateCreated>
								 * <description></description>
								 * <multiSelect>true</multiSelect>
								 * <name>Кому</name>
								 * <obligatory>true</obligatory>
								 * <organizationTag>user</organizationTag>
								 * <organizationValue
								 * >fb926d69-3a49-4842-a2e2-e592fd301073
								 * </organizationValue>
								 * <type>ORGANIZATION</type>
								 * <computationalConfirm
								 * >NONE</computationalConfirm>
								 * <computationalReadonly
								 * >false</computationalReadonly>
								 * <code>Кому</code> <xmlAttributes/>
								 * </xmlAttribute>
								 */
								String organizationValue = get(att_list_element, "organizationValue", null);
								String organizationTag = get(att_list_element, "organizationTag", null);

								if (organizationValue != null)
								{
									if (organizationTag.indexOf("user") >= 0)
									{
										addPersonToDocument(newDocSubj, organizationValue, onto_code);

									}
								}
							} else if (type.equals("TEXT"))
							{

								String textValue = get(att_list_element, "textValue", null);

								// if (textValue != null && textValue.length() >
								// 0)
								// writeTriplet(newDocSubj, onto_code,
								// textValue, true);
							} else if (type.equals("LINK"))
							{
								String value = get(att_list_element, "linkValue", null);

								if (value != null && value.length() > 0)
								{
									addLinkToDocument(newDocSubj, value, onto_code);
								}

							}

						}
					}

				} catch (Exception ex)
				{
					ex.printStackTrace();
					System.out.println("skip document id=" + id + ", reson:" + ex.getMessage());
				}
			}
			docRecordRs.close();
			st1.close();

		}

		return count_documents;
	}

	/**
	 * Выгружает данные орг. структуры в виде триплетов
	 */
	private static void fetchOrganization(PacahonClient pacahon_client, String ticket)
	{
		/*
		 * список organization-roots спизжен из из конфига client.xml:
		 */
		HashMap<String, String> roots = new HashMap<String, String>();

		roots.put("92e57b6d-83e3-485f-8885-0bade363f759", "Y");
		roots.put("120c92a1-c738-4297-95ba-7a624a1343f7", "Y");
		roots.put("5fca919f-ddd3-41a7-afe1-53daac507bee", "Y");
		roots.put("f4c6c428-faec-40b3-b870-de70f0877ca7", "Y");
		roots.put("59f99bce-32b8-4442-b766-def30f4194c1", "Y");
		roots.put("7289c9ec-f45a-40ba-b7ae-41c66ed71aee", "Y");
		roots.put("fb6583b7-ed14-492f-bf92-45eb3cab3a56", "Y");
		roots.put("926abd62-c00d-4f9e-8e36-dcb76aa0c23a", "Y");
		roots.put("a5e73ead-a54d-4877-9b8d-8ee4f2b392a6", "Y");
		roots.put("b6f88416-e1ee-4839-a6a2-aa6e9f6b07d1", "Y");
		roots.put("d0f7131e-affb-4227-974a-71e3b39d71bd", "Y");
		roots.put("7822d046-2da0-43dc-9288-ab197a2b7d97", "Y");
		roots.put("6108a193-14c0-4e93-970b-a1ed29d55550", "Y");
		roots.put("7f7c7821-8f6a-4fbb-9f3e-b09c0f7f1d05", "Y");
		roots.put("2b01de3d-0d1f-458d-a62b-451037c0a5c7", "Y");
		roots.put("5bf316d8-2424-4780-a81b-262214543f61", "Y");
		roots.put("2e5c4e09-4439-45c6-8589-0cdcb9da6c03", "Y");

		try
		{
			String structure_id = "0";

			long start = System.currentTimeMillis();

			// prepareRoles();
			// populateAdmins();

			List<Department> deps = organizationUtil.getDepartments();

			System.out.print(deps);

			ArrayList<String> excludeNode = new ArrayList<String>();
			excludeNode.add("1154685117926");// 14dd8e2e-634f-4332-bc4d-4bc708a9ff64:1154685117926:_ТЕЛЕФОНЫ
												// СПЕЦВЫЗОВА
			// excludeNode.add("1146725963873");
			// excludeNode.add("1000");
			// excludeNode.add("123456");

			// находим родителей для всех подразделений
			int buCounter = 0;
			Map<String, Department> departmentsOfExtIdMap = new HashMap<String, Department>();
			// HashMap<String, ArrayList<String>> childs = new HashMap<String,
			// ArrayList<String>>();
			HashMap<String, String> childToParent = new HashMap<String, String>();
			for (Department department : deps)
			{
				if (department.getExtId() == null)
					continue;

				if (excludeNode.contains(department.getExtId()) == true)
					continue;

				List<Department> childDeps = organizationUtil.getDepartmentsByParentId(department.getExtId(), "Ru");

				// ArrayList<String> breed = new ArrayList<String>();
				for (Department child : childDeps)
				{
					// breed.add(child.getInternalId());
					childToParent.put(child.getExtId(), department.getExtId());
				}
				// childs.put(department.getId(), breed);

				departmentsOfExtIdMap.put(department.getExtId(), department);
				buCounter++;
			}

			// установим для каждого из подразделений его организацию
			for (Department department : deps)
			{

				String parent = childToParent.get(department.getExtId());

				if (parent == null)
					continue;

				String up_department = null;

				while (parent != null)
				{
					up_department = parent;

					parent = childToParent.get(parent);
				}

				department.setOrganizationId(up_department);
			}

			// выгружаем штатное расписание и сотрудников
			buCounter = 0;

			System.out.println("выгружаем штатное расписание и сотрудников");

			int ii = 0;

			for (Department department : deps)
			{
				ii++;

				if (department.getExtId() == null)
				{
					System.out.println("exclude node:" + department.getExtId());
					continue;
				}

				if (excludeNode.contains(department.getExtId()) == true)
				{
					System.out.println("exclude node:" + department.getExtId());
					continue;
				}

				String parent = childToParent.get(department.getExtId());
				boolean isOrganization = false;
				boolean isDepartment = false;
				boolean isGroup = false;

				if (parent == null || group__Id_name.get(department.getId()) != null)
				{
					isGroup = true;
				} else if (parent.equals(structure_id))
				{
					isOrganization = true;
				} else
				{
					isDepartment = true;
				}

				if (parent != null && parent.equals(structure_id) && roots.get(department.getId()) == null)
				{
					parent = null;
				}
				if (roots.get(department.getId()) != null)
				{
					parent = "0";
				}

				Model node = ModelFactory.createDefaultModel();
				node.setNsPrefixes(predicates.getPrefixs());

				Resource r_department = null;
				Resource r = null;
				if (isDepartment == true)
				{
					System.out.println(ii + " add department " + department.getNameRu());

					r_department = node.createResource(predicates.zdb + "dep_" + department.getExtId());
					r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.swrc__Department));

					if (department.isActive())
					{
						r_department.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r = node.createResource(predicates.zdb + "doc_" + department.getId());
					r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.docs__department_card));

					if (department.isActive())
					{
						r.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
							node.createLiteral(department.getNameRu(), "ru"));

					if (department.getNameEn() != null)
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
								node.createLiteral(department.getNameEn(), "en"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.docs__unit),
							ResourceFactory.createProperty(predicates.zdb, "dep_" + department.getExtId()));

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__organization),
							ResourceFactory.createProperty(predicates.zdb, "org_" + department.getOrganizationId()));

					r.addProperty(ResourceFactory.createProperty(predicates.gost19__externalIdentifer),
							node.createLiteral(department.getExtId()));

				} else if (isOrganization)
				{
					System.out.println(ii + " add organization " + department.getNameRu());

					r_department = node.createResource(predicates.zdb + "org_" + department.getExtId());
					r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.swrc__Organization));

					if (department.isActive())
					{
						r_department.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r = node.createResource(predicates.zdb + "doc_" + department.getId());
					r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.docs__organization_card));

					if (department.isActive())
					{
						r.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.gost19__externalIdentifer),
							node.createLiteral(department.getExtId()));

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
							node.createLiteral(department.getNameRu(), "ru"));

					if (department.getNameEn() != null)
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
								node.createLiteral(department.getNameEn(), "en"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__organization),
							ResourceFactory.createProperty(predicates.zdb, "org_" + department.getExtId()));

				} else if (isGroup)
				{

					System.out.println(ii + " add group " + department.getNameRu());

					r_department = node.createResource(predicates.zdb + "group_" + department.getExtId());
					r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.docs__Group));

					if (department.isActive())
					{
						r_department.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r = node.createResource(predicates.zdb + "doc_" + department.getId());
					r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.docs__group_card));

					if (department.isActive())
					{
						r.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
							node.createLiteral(department.getNameRu(), "ru"));

					if (department.getNameEn() != null)
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__name),
								node.createLiteral(department.getNameEn(), "en"));
					}

					r.addProperty(ResourceFactory.createProperty(predicates.docs__unit),
							ResourceFactory.createProperty(predicates.zdb, "dep_" + department.getExtId()));

					r.addProperty(ResourceFactory.createProperty(predicates.gost19, "externalIdentifer"),
							node.createLiteral(department.getExtId()));
				}
				if (parent != null)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__parentUnit),
							ResourceFactory.createProperty(predicates.zdb, "dep_" + parent));

					write_add_info_of_attribute(predicates.zdb, "doc_" + department.getId(), predicates.docs,
							"parentUnit", predicates.zdb, "dep_" + parent, predicates.swrc, "name",
							departmentsOfExtIdMap.get(parent).getNameRu(), node);
				}

				// TODO возможно нужно будет записать и детишек
				// for (String child : childs.get(department.getId()))
				// {
				// writeTriplet(department.getId(), predicates.HAS_PART,
				// newToInternalIdMap.get(child), true);
				// }

				// break;
				pacahon_client.put(ticket, node);
			}

			long end = System.currentTimeMillis();
			System.out.println("Finished in " + ((end - start) / 1000) + " s. for " + deps.size() + " departments.");
			System.out.println("Querying speed  = " + deps.size() / ((end - start) / 1000) + " deps/s");

			// Выгружаем пользователей

			start = System.currentTimeMillis();

			List<EntityType> list = organizationUtil.getUsers();
			ii = 0;
			for (EntityType userEntity : list)
			{
				ii++;

				if (ii % 1000 == 0)
					Thread.sleep(1000);

				String userId = userEntity.getUid();
				System.out.println(ii + " add user " + userId);

				userUri__userObj.put(predicates.zdb + "person_" + userId, userEntity);

				Model node = ModelFactory.createDefaultModel();
				node.setNsPrefixes(predicates.getPrefixs());

				Resource r_user = node.createResource(predicates.zdb + "person_" + userId);
				r_user.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
						ResourceFactory.createProperty(predicates.swrc__Employee));

				Resource r = node.createResource(predicates.zdb + "doc_" + userId);
				r.addProperty(ResourceFactory.createProperty(predicates.rdf, "type"),
						ResourceFactory.createProperty(predicates.docs, "employee_card"));

				r.addProperty(ResourceFactory.createProperty(predicates.gost19, "employee"),
						ResourceFactory.createProperty(predicates.zdb, "person_" + userId));

				String domainName = null;
				String password = null;

				for (AttributeType a : userEntity.getAttributes().getAttributeList())
				{
					if (a.getName().equalsIgnoreCase("active") && a.getValue().equalsIgnoreCase("true"))
					{
						r_user.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
						r.addProperty(ResourceFactory.createProperty(predicates.docs__active),
								node.createLiteral("true"));
					}

					if (a.getName().equalsIgnoreCase("firstNameRu"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__firstName),
								node.createLiteral(a.getValue(), "ru"));
					} else if (a.getName().equalsIgnoreCase("firstNameEn"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__firstName),
								node.createLiteral(a.getValue(), "en"));
					} else if (a.getName().equalsIgnoreCase("secondnameRu"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.gost19__middleName),
								node.createLiteral(a.getValue(), "ru"));
					} else if (a.getName().equalsIgnoreCase("secondnameEn"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.gost19__middleName),
								node.createLiteral(a.getValue(), "en"));
					} else if (a.getName().equalsIgnoreCase("surnameRu"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__lastName),
								node.createLiteral(a.getValue(), "ru"));
					} else if (a.getName().equalsIgnoreCase("surnameEn"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__lastName),
								node.createLiteral(a.getValue(), "en"));
					} else if (a.getName().equals("domainName"))
					{
						domainName = a.getValue();
					} else if (a.getName().equals("password"))
					{
						password = a.getValue();
					} else if (a.getName().equals("email"))
					{
						String email = a.getValue();
						if (email != null && email.length() > 0)
							r.addProperty(ResourceFactory.createProperty(predicates.swrc__email),
									node.createLiteral(a.getValue()));
					} else if (a.getName().equalsIgnoreCase("id"))
					{
						// writeTriplet(p.zdb + "doc_" + userId,
						// "magnet-ontology#id", a.getValue(), true);
					} else if (a.getName().equalsIgnoreCase("pid"))
					{
						// writeTriplet(p.zdb + "doc_" + userId,
						// "magnet-ontology#pid", a.getValue(), true);
					} else if (a.getName().equalsIgnoreCase("pager"))
					{
						String value = a.getValue();
						if (value != null && value.length() > 0)
							r.addProperty(ResourceFactory.createProperty(predicates.docs__pager),
									node.createLiteral(a.getValue()));
					} else if (a.getName().equalsIgnoreCase("phone"))
					{
						String value = a.getValue();
						if (value != null && value.length() > 0)
							r.addProperty(ResourceFactory.createProperty(predicates.swrc__phone),
									node.createLiteral(a.getValue()));
					} else if (a.getName().equalsIgnoreCase("offlineDateBegin"))
					{
						// writeTriplet(p.zdb + "doc_" + userId,
						// "magnet-ontology#offlineDateBegin", a.getValue(),
						// true);
					} else if (a.getName().equalsIgnoreCase("offlineDateEnd"))
					{
						// writeTriplet(p.zdb + "doc_" + userId,
						// "magnet-ontology#offlineDateEnd", a.getValue(), true,
						// out);
					} else if (a.getName().equalsIgnoreCase("departmentId"))
					{
						Department department = departmentsOfExtIdMap.get(a.getValue());

						if (department == null)
							System.out.println("dep is null for user (id = " + userId + ")");
						else
						{
							r.addProperty(ResourceFactory.createProperty(predicates.docs__unit),
									ResourceFactory.createProperty(predicates.zdb, "dep_" + department.getExtId()));

							write_add_info_of_attribute(predicates.zdb, "doc_" + userId, predicates.docs, "unit",
									predicates.zdb, "dep_" + department.getExtId(), predicates.swrc, "name",
									department.getNameRu(), node);
						}

					} else if (a.getName().equalsIgnoreCase("mobilePrivate"))
					{
						String value = a.getValue();
						// if (value != null && value.length() > 0)
						// writeTriplet(predicates.zdb + "doc_" + userId,
						// predicates.swrc__phone, a.getValue(), true,
						// out);
					} else if (a.getName().equalsIgnoreCase("phoneExt"))
					{
						String value = a.getValue();
						// if (value != null && value.length() > 0)
						// writeTriplet(predicates.zdb + "doc_" + userId,
						// predicates.swrc__phone, a.getValue(), true,
						// out);
					} else if (a.getName().equalsIgnoreCase("mobile"))
					{
						String value = a.getValue();
						if (value != null && value.length() > 0)
							r.addProperty(ResourceFactory.createProperty(predicates.swrc__phone),
									node.createLiteral(a.getValue()));
					} else if (a.getName().equalsIgnoreCase("employeeCategoryR3"))
					{
						// writeTriplet(userId,
						// "magnet-ontology#employeeCategoryR3", a.getValue(),
						// true);
					} else if (a.getName().equalsIgnoreCase("r3_ad"))
					{
						// writeTriplet(userId, "magnet-ontology#r3_ad",
						// a.getValue(), true);
					} else if (a.getName().equalsIgnoreCase("photo"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.swrc__photo),
								node.createLiteral(a.getValue()));

						// writeTriplet(userId,
						// "http://swrc.ontoware.org/ontology#photo",
						// a.getValue(), true);
					} else if (a.getName().equalsIgnoreCase("photoUID"))
					{
						// writeTriplet(userId, "magnet-ontology#photoUID",
						// a.getValue(), true);
					} else if (a.getName().equalsIgnoreCase("postRu"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.docs__position),
								node.createLiteral(a.getValue(), "ru"));
					} else if (a.getName().equalsIgnoreCase("postEn"))
					{
						r.addProperty(ResourceFactory.createProperty(predicates.docs__position),
								node.createLiteral(a.getValue(), "en"));
					} else if (a.getName().equalsIgnoreCase("administrator"))
					{
						// if (a.getValue().equals("1"))
						// {
						// a.setValue("true");
						// }
						// else if (!(a.getValue().equals("true") ||
						// a.getValue().equals("false")))
						// {
						// a.setValue("false");
						// }
						// writeTriplet(userId, p.IS_ADMIN, a.getValue(), true,
						// out);
					}
					// else if (!writeRole(prefix, a))
					// {
					// writeTriplet(prefix, "magnet-ontology#unknown"
					// + a.getName(), a.getValue(), true);
					// }
				}

				if (domainName != null && password != null)
				{
					// обяьвим этого субьекта как аутентифицируемого и добавим
					// необходимые данные, выгружаем в отдельный файл

					r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.auth__Authenticated));

					r.addProperty(ResourceFactory.createProperty(predicates.auth__login),
							node.createLiteral(domainName));

					// writeTriplet(predicates.zdb + "person_" + userId,
					// predicates.auth__credential, password, true,
					// out_auth_data);
				}

				pacahon_client.put(ticket, node);
			}

			end = System.currentTimeMillis();
			System.out.println("Finished in " + ((end - start) / 1000) + " s. for " + list.size() + " persons.");
			/*
			 * System.out.println("Querying speed  = " + list.size() / ((end -
			 * start + 1) / 1000) + " persons/s");
			 */

			System.out.println("-----------------------------------------");

		} catch (Exception e)
		{

			System.out.println("Error !");

			e.printStackTrace();

			printUsage();

		}
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	static void init_source() throws Exception
	{
		System.out.print("connect to source database " + dbUrl + "...");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl, dbUser, dbPassword);

		System.out.println("ok");
	}

	private static void addLinkToDocument(String DocUri, String DocId, String attUri) throws Exception
	{
		String PersonUri = predicates.zdb + DocId;

		// writeTriplet(DocUri, attUri, PersonUri, false);
		String newNodeId = DocUri + attUri;

		// writeTriplet(newNodeId, predicates.rdf__type,
		// predicates.rdf__Statement, false);
		// writeTriplet(newNodeId, predicates.rdf__object, PersonUri, false);
		// writeTriplet(newNodeId, predicates.rdf__subject, DocUri, false);
		// writeTriplet(newNodeId, predicates.rdf__predicate, attUri, false);

		// writeTriplet(newNodeId, predicates.swrc__firstName,
		// "репрезентатионвалуес", false);
	}

	private static void addPersonToDocument(String DocUri, String PersonId, String attUri) throws Exception
	{
		String PersonUri = predicates.zdb + "person_" + PersonId;

		// writeTriplet(DocUri, attUri, PersonUri, false);

		String newNodeId = DocUri + attUri;

		// writeTriplet(newNodeId, predicates.rdf__type,
		// predicates.rdf__Statement, false);
		// writeTriplet(newNodeId, predicates.rdf__object, PersonUri, false);
		// writeTriplet(newNodeId, predicates.rdf__subject, DocUri, false);
		// writeTriplet(newNodeId, predicates.rdf__predicate, attUri, false);

		EntityType person = userUri__userObj.get(PersonUri);
		if (person == null)
		{
			person = organizationUtil.getUser(PersonId);
		}

		if (person != null)
		{
			for (AttributeType a : person.getAttributes().getAttributeList())
			{
				String name = a.getName();
				String value = a.getValue();

				if (name.equalsIgnoreCase("firstNameRu"))
				{
					// if (!(PersonId.equals(user_docflow) && (value == null ||
					// value.length() == 0)))
					// writeTriplet(newNodeId, predicates.swrc__firstName,
					// value, false);
				} else if (name.equalsIgnoreCase("surnameRu"))
				{
					// writeTriplet(newNodeId, predicates.swrc__lastName, value,
					// false);
				}
			}
		} else
		{
			throw new Exception("user [" + PersonId + "] not found in organization");
		}
	}

	private static void loadProperties()
	{

		try
		{
			properties.load(new FileInputStream("ba2pacahon.properties"));

			documentTypeId = properties.getProperty("documentTypeId", "");
			ticketId = properties.getProperty("sessionTicketId", "");
			SEARCH_URL = properties.getProperty("searchUrl", "");
			// DOCUMENT_SERVICE_URL = properties.getProperty("documentsUrl",
			// "");
			// fake = new Boolean(properties.getProperty("fake", "false"));
			pathToDump = properties.getProperty("pathToDump");
			dbUser = properties.getProperty("dbUser", "ba");
			dbPassword = properties.getProperty("dbPassword", "123456");
			dbUrl = properties.getProperty("dbUrl", "localhost:3306");
			dbSuffix = properties.getProperty("dbSuffix", "");
		} catch (IOException e)
		{
			writeDefaultProperties();
		}

	}

	private static void writeDefaultProperties()
	{

		System.out.println("Writing default properties.");

		properties.setProperty("documentTypeId", "fake-type-2mn3-6n3m");
		properties.setProperty("sessionTicketId", "fake-tiket-4abe-8c5f-a30d6c251165");
		properties.setProperty("searchUrl", "http://localhost:9874/ba-server/SearchServices?wsdl");
		properties.setProperty("documentsUrl", "http://localhost:9874/ba-server/DocumentServices?wsdl");
		properties.setProperty("fake", "false");
		properties.setProperty("pathToDump", "data");
		properties.setProperty("organizationName", "OrganizationEntityService");
		properties.setProperty("organizationNameSpace", "http://organization.magnet.magnetosoft.ru/");
		properties.setProperty("organizationUrl", "http://localhost:9874/organization/OrganizationEntitySvc?wsdl");
		properties.setProperty("dbUser", "ba");
		properties.setProperty("dbPassword", "123456");
		properties.setProperty("dbUrl", "localhost:3306");
		properties.setProperty("dbSuffix", "");

		try
		{
			properties.store(new FileOutputStream("ba2pacahon.properties"), null);
		} catch (IOException e)
		{
		}
	}

	private static void printUsage()
	{
		System.out
				.println("Usage  : java -cp ba2pacahon.jar org.gost19.ba2pacahon.Fetcher [ fetchType(directive|organization) [ docType [ pathToDump [ ticketId [ searchServicesWsdl [ searchQname [ docUrl [ fake(if exists => true) ] ] ] ] ] ]");
		System.out.println("Example: java -cp ba2pacahon.jar org.gost19.ba2pacahon.Fetcher " + documentTypeId + " "
				+ pathToDump + " " + ticketId + " ");
	}

	public static String escape(String string)
	{

		if (string != null)
		{
			StringBuilder sb = new StringBuilder();

			char c = ' ';

			for (int i = 0; i < string.length(); i++)
			{

				c = string.charAt(i);

				if (c == '\n')
				{
					sb.append("\\n");
				} else if (c == '\r')
				{
					sb.append("\\r");
				} else if (c == '\\')
				{
					sb.append("\\\\");
				} else if (c == '"')
				{
					sb.append("\\\"");
				} else if (c == '\t')
				{
					sb.append("\\t");
				} else
				{
					sb.append(c);
				}
			}
			return sb.toString();
		} else
		{
			return "";
		}
	}

	private static void write_add_info_of_attribute(String subject_ns, String subject_id, String predicate_ns,
			String predicate_id, String object_ns, String object_id, String addInfo_predicate_ns,
			String addInfo_predicate_id, String addInfo_value, Model node) throws Exception
	{
		String addinfo_subject = subject_ns + subject_id + "_add_info_" + System.currentTimeMillis();

		Resource r_department = node.createResource(addinfo_subject);

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
				ResourceFactory.createProperty(predicates.rdf, "Statement"));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__subject),
				ResourceFactory.createProperty(subject_ns, subject_id));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__predicate),
				ResourceFactory.createProperty(predicate_ns, predicate_id));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__object),
				ResourceFactory.createProperty(object_ns, object_id));

		r_department.addProperty(ResourceFactory.createProperty(addInfo_predicate_ns, addInfo_predicate_id),
				node.createLiteral(addInfo_value));
	}

	private static String renameCodeToOnto(String code, String id, String alternativeName)
	{
		String this_code_in_onto = code_onto.get(code);

		if (this_code_in_onto == null)
		{
			if ((code.indexOf('1') >= 0 || code.indexOf('2') >= 0 || code.indexOf('3') >= 0 || code.indexOf('4') >= 0
					|| code.indexOf('5') >= 0 || code.indexOf('6') >= 0 || code.indexOf('7') >= 0
					|| code.indexOf('8') >= 0 || code.indexOf('9') >= 0 || code.indexOf('0') >= 0)
					&& alternativeName != null)
			{
				// если code содержит uid, то заменим code на
				// название из метки
				if (id.equals("0027562cbe0948e5965c3183eb23e42c") == false)
					this_code_in_onto = alternativeName;
			}

			this_code_in_onto = predicates.user_onto
					+ Translit.toTranslit(code).replace(' ', '_').replace('\'', 'j').replace('№', 'N')
							.replace(',', '_').replace('(', '_').replace(')', '_').replace('.', '_');

		}

		old_code__new_code.put(code, this_code_in_onto);

		return this_code_in_onto;
	}

	private static String get(IXMLElement el, String Name, String def_val)
	{
		String res = null;

		IXMLElement dd = el.getFirstChildNamed(Name);

		if (dd != null)
			res = dd.getContent();

		if (res == null)
			return def_val;
		else
			return res;
	}

}
