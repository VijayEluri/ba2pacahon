package org.gost19.ba2pacahon;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class Fetcher
{
	private static String documentTypeId;
	private static String ticketId;
	private static String pathToDump;
	private static Properties properties = new Properties();
	private static Connection connection = null;
	private static String dbUser;
	private static String dbPassword;
	private static String dbUrl;
	private static HashMap<String, String> code_onto = new HashMap<String, String>();
	private static HashMap<String, String> old_code__new_code = new HashMap<String, String>();
	private static String exclude_codes_template = "$comment, $private, $authorId, $defaultRepresentation";
	private static String exclude_codes_doc = "$private";
	private static HashMap<String, Object> ouUri__userObj = new HashMap<String, Object>();
	private static OrganizationUtil organizationUtil;

	private static HashMap<String, String> group__Id_name;
	private static HashMap<String, String> recordId__versionId = new HashMap<String, String>();
	private static HashMap<String, String> docUri__templateUri = new HashMap<String, String>();

	private static Map<String, Department> departmentsOfExtIdMap = new HashMap<String, Department>();

	private static Map<String, String[]> templateId__defaultRepresentation = new HashMap<String, String[]>();
	private static Map<String, String[]> templateUri_fieldUri__takedUri = new HashMap<String, String[]>();

	/**
	 * Выгружает данные структуры документов в виде пользовательских онтологий
	 */

	private static void fetchDocumentTypes(PacahonClient pacahon_client, String ticket) throws Exception
	{
		IXMLParser parser = XMLParserFactory.createDefaultXMLParser();

		HashMap<String, Integer> code_stat = new HashMap<String, Integer>();

		// versionId__recordId = new HashMap<String, String>();
		// recordId__versionId = new HashMap<String, String>();

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

		ResultSet templatesRs = connection.createStatement().executeQuery(
				"select objectId FROM objects where isTemplate = 1 and timestamp is null");

		while (templatesRs.next())
		{
			String docId = templatesRs.getString(1);
			// System.out.println("templateId = " + docId);

			String docDataQuery = "select distinct content, kindOf, timestamp, recordId FROM objects where objectId = '"
					+ docId + "' order by timestamp desc";
			Statement st1 = connection.createStatement();
			ResultSet templateRecordRs = st1.executeQuery(docDataQuery);

			int count_of_version = 0;

			while (templateRecordRs.next())
			{
				Model node = ModelFactory.createDefaultModel();
				node.setNsPrefixes(predicates.getPrefixs());

				Resource r = null;

				count_of_version++;
				String docXmlStr = templateRecordRs.getString(1);
				int kindOf = templateRecordRs.getInt(2);

				Object timestamp = templateRecordRs.getObject(3);

				String recordId = templateRecordRs.getString(4);

				IXMLReader reader = StdXMLReader.stringReader(docXmlStr);
				// System.out.println(docXmlStr);

				parser.setReader(reader);
				IXMLElement xmlDoc = (IXMLElement) parser.parse(true);
				reader.close();

				String authorId = util.get(xmlDoc, "authorId", null);
				String dateCreated = util.get(xmlDoc, "dateCreated", null);
				String lastModifiedTime = util.get(xmlDoc, "dateLastModified", null);
				// String lastEditorId = get(xmlDoc, "lastEditorId", null);

				String objectType = util.get(xmlDoc, "objectType", null);
				// String typeId = get(xmlDoc, "typeId", null);
				String active = util.get(xmlDoc, "active", null);

				String name = util.get(xmlDoc, "name", null);
				System.out.println(name);
				System.out.println("							" + lastModifiedTime);

				String systemInformation = util.get(xmlDoc, "systemInformation", null);
				String[] def_repr_code = null;

				if (systemInformation != null)
				{
					for (String el : systemInformation.split(";"))
					{
						if (el.indexOf("$defaultRepresentation") == 0)
						{
							def_repr_code = new String[1];
							def_repr_code[0] = el.split("=")[1];
						}
					}
				}
				// System.out.println("systemInformation = [" +
				// systemInformation + "]");

				// XMLGregorianCalendar dateCreated =
				// documentTypeType.getDateCreated();
				// XMLGregorianCalendar lastModifiedTime =
				// documentTypeType.getLastModifiedTime();
				// String name = documentTypeType.getName();
				// String systemInformation =
				// documentTypeType.getSystemInformation();

				if (dateCreated != null && lastModifiedTime != null)
				{
					if (dateCreated.equals(lastModifiedTime))
					{
						// System.out.println("dateCreated == lastModifiedTime");
					} else
					{
						// System.out.println("dateCreated != lastModifiedTime");
					}
				}

				// String activeStatusLabel = "";
				// if (dateCreated == null)
				// activeStatusLabel = "удален";

				// String draftStatusLabel = "";
				// if (documentTypeType.isInDraftState() == true)
				// draftStatusLabel = "черновик";

				// System.out.println(String.format("\n%s:[%s] %s %s",
				// documentTypeType.getId(), name, activeStatusLabel,
				// draftStatusLabel));

				// String[] si_elements = null;

				// if (systemInformation != null)
				// si_elements = systemInformation.split(";");

				String id = util.get(xmlDoc, "id", null);

				String tmplate_id = null;

				if (objectType.equals("DICTIONARY"))
				{
					tmplate_id = predicates.user_onto + "tmplDict_" + id + "_v_" + count_of_version;
				} else
				{
					tmplate_id = predicates.user_onto + "template_" + id + "_v_" + count_of_version;
				}

				System.out.println("recordId = " + recordId + ", tmplate_id = " + tmplate_id);

				recordId__versionId.put(recordId, tmplate_id);
				// versionId__recordId.put(tmplate_id, recordId);

				r = node.createResource(tmplate_id);

				r.addProperty(ResourceFactory.createProperty(predicates.rdfs__subClassOf),
						ResourceFactory.createProperty(predicates.docs__Document));

				r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
						ResourceFactory.createProperty(predicates.rdfs__Class));

				r.addProperty(ResourceFactory.createProperty(predicates.dc__identifier), node.createLiteral(id));

				r.addProperty(ResourceFactory.createProperty(predicates.gost19__version),
						node.createLiteral(count_of_version + ""));

				if (systemInformation != null)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.gost19__representation),
							node.createLiteral(systemInformation));
				}

				if (kindOf == 0)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__kindOf),
							node.createLiteral("user_template"));
				} else if (kindOf == 1)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__kindOf),
							node.createLiteral("dictionary_template"));
				} else if (kindOf == 3)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__kindOf),
							node.createLiteral("report_form_template"));
				}

				if (timestamp == null)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__actual), node.createLiteral("true"));
				}

				if (active.equals("true"))
				{
					r.addProperty(ResourceFactory.createProperty(predicates.docs__active), node.createLiteral("true"));
				}

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

						String att_name = util.get(att_list_element, "name", "");
						String textValue = util.get(att_list_element, "textValue", null);

						if (att_name.equals("$comment"))
						{

							if (textValue != null)
							{
								r.addProperty(ResourceFactory.createProperty(predicates.rdfs__comment),
										node.createLiteral(textValue, "ru"));
							}

							continue;
						}

						if (exclude_codes_template.indexOf(att_name) >= 0)
						{
							// System.out.println("\n	att_name=[" + att_name +
							// "] is skipped");

							continue;
						}

						String restrictionId = predicates.user_onto + "template_" + id + "_v_" + count_of_version
								+ "_f_" + ii;

						// System.out.println("\n");

						Resource rr = node.createResource(restrictionId);
						rr.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
								ResourceFactory.createProperty(predicates.owl__Restriction));

						if (textValue != null)
						{
							r.addProperty(ResourceFactory.createProperty(predicates.owl__hasValue),
									node.createLiteral(textValue));
						}

						r.addProperty(ResourceFactory.createProperty(predicates.gost19__isRelatedTo),
								node.createProperty(predicates.docs__Document));

						rr.addProperty(ResourceFactory.createProperty(predicates.gost19__isRelatedTo),
								node.createProperty(predicates.docs__Document));

						rr.addProperty(ResourceFactory.createProperty(predicates.dc__hasPart),
								node.createProperty(tmplate_id));

						r.addProperty(ResourceFactory.createProperty(predicates.rdfs__subClassOf),
								node.createProperty(restrictionId));

						lName = LangString.parse(att_name);

						if (lName.text_ru != null)
							rr.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
									node.createLiteral(lName.text_ru, "ru"));

						if (lName.text_en != null)
							rr.addProperty(ResourceFactory.createProperty(predicates.rdfs__label),
									node.createLiteral(lName.text_en, "en"));

						String code = util.get(att_list_element, "code", null);

						// System.out.println("	code=[" + code + "]");

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

						rr.addProperty(ResourceFactory.createProperty(predicates.dc__identifier),
								node.createLiteral(code));

						code_stat.put(code, count);

						String descr = util.get(att_list_element, "description", "");

						if (descr.length() > 1)
						{
							rr.addProperty(ResourceFactory.createProperty(predicates.gost19__representation),
									node.createLiteral(descr));
						}

						// System.out.println("att_description=[" + descr +
						// "]");
						// computationalConfirm
						// computationalReadonly
						// computationalRuleName

						String multi_select_label = "";
						String obligatory_label = "";

						obligatory_label = util.get(att_list_element, "obligatory", "false");
						multi_select_label = util.get(att_list_element, "multiSelect", "false");

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

						String type = util.get(att_list_element, "type", "");

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
								String dictionaryIdValue = util.get(att_list_element, "dictionaryIdValue", null);

								if (dictionaryIdValue != null)
								{
									// тип спраочника
									obj_owl__allValuesFrom = predicates.user_onto + "tmplDict_" + dictionaryIdValue
											+ "_v_1";

									String dictionaryNameValue = util
											.get(att_list_element, "dictionaryNameValue", null);

									if (dictionaryNameValue != null)
									{
										write_add_info_of_attribute(restrictionId, predicates.owl__allValuesFrom,
												obj_owl__allValuesFrom, predicates.swrc__name, dictionaryNameValue,
												node);
									}

									// Значение по умолчанию
									String recordIdValue = util.get(att_list_element, "recordIdValue", null);

									if (recordIdValue != null)
									{
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__hasValue),
												node.createLiteral(recordIdValue));
									}

									String recordNameValue = util.get(att_list_element, "recordNameValue", null);

									if (recordNameValue != null)
									{
										write_add_info_of_attribute(restrictionId, predicates.owl__hasValue,
												obj_owl__allValuesFrom, predicates.swrc__name, recordNameValue, node);
									}

									// r.addProperty(ResourceFactory.createProperty(predicates.docs__kindOf),
									// node.createLiteral("dictionary_template"));
								}
							} else
							{
								obj_owl__allValuesFrom = predicates.docs__Document;
							}

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

											obj_owl__allValuesFrom = predicates.user_onto + "template_" + id + "_v_1";
										}
									}
									if (compz.indexOf("$composition") >= 0)
									{
										String[] tmpa = compz.split("=");
										if (tmpa.length > 1)
										{
											String def_rr = null;

											if (def_repr_code != null)
												def_rr = def_repr_code[0];

											String data = tmpa[1];
											if (data != null)
											{
												String[] importsFields = data.replace('|', ';').split(";");

												if (def_rr != null && def_rr.equals(code))
												{
													// это поле - значение по
													// умолчанию для документа,
													// да к тому-же составное
													def_repr_code = new String[importsFields.length];
												}

												String[] take = new String[importsFields.length];
												int i = 0;
												int j = 0;
												for (String field : importsFields)
												{
													String new_code = old_code__new_code.get(field);

													if (new_code == null)
														new_code = renameCodeToOnto(field, null, null);

													if (def_rr != null && def_rr.equals(code))
													{
														// это поле - значение
														// по умолчанию для
														// документа, да к
														// тому-же составное
														def_repr_code[i] = field;
														i++;
													}

													rr.addProperty(
															ResourceFactory.createProperty(predicates.gost19__take),
															node.createLiteral(new_code));
													take[j] = new_code;
													j++;
												}

												templateUri_fieldUri__takedUri.put(
														tmplate_id + "+" + this_code_in_onto, take);

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
							obj_owl__allValuesFrom = predicates.xsd__decimal;
						} else if (type.equals("ORGANIZATION"))
						{
							typeLabel = "organization";
							// obj_owl__allValuesFrom =
							// Predicate.swrc__Organization;

							// нужно определить что это за тип, person ?
							// department ? organization
							String organizationTag = util.get(att_list_element, "organizationTag", "");

							String[] organizationTags = organizationTag.split(";");

							for (String tag : organizationTags)
							{
								if (tag.equals("user"))
								{
									// для этого типа нужно добавить:

									rr.addProperty(ResourceFactory.createProperty(predicates.gost19__take),
											ResourceFactory.createProperty(predicates.swrc__lastName));

									rr.addProperty(ResourceFactory.createProperty(predicates.gost19__take),
											ResourceFactory.createProperty(predicates.swrc__firstName));

									if (organizationTags.length == 1)
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__allValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Person));
									else
										rr.addProperty(ResourceFactory.createProperty(predicates.owl__someValuesFrom),
												ResourceFactory.createProperty(predicates.swrc__Person));

								} else if (tag.equals("department"))
								{
									rr.addProperty(ResourceFactory.createProperty(predicates.gost19__take),
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
							obj_owl__allValuesFrom = predicates.xsd__string;
						}

						if (obj_owl__allValuesFrom != null)
						{
							try
							{
								rr.addProperty(ResourceFactory.createProperty(predicates.owl__allValuesFrom),
										ResourceFactory.createProperty(obj_owl__allValuesFrom));
							} catch (Exception ex)
							{
								throw ex;
							}
						}

						// System.out.println("	att_name=[" + att_name + "]:" +
						// typeLabel + " " + obligatory_label + " "
						// + multi_select_label);
						// System.out.println("	description=[" + descr + "]");
						//
					}

					if (def_repr_code != null)
					{
						for (int i = 0; i < def_repr_code.length; i++)
						{
							String new_code = old_code__new_code.get(def_repr_code[i]);
							if (new_code == null)
								System.out.print("?");

							def_repr_code[i] = new_code;
						}

						templateId__defaultRepresentation.put(tmplate_id, def_repr_code);
					}

					pacahon_client.put(ticket, node);
				}

			}
			templateRecordRs.close();
			st1.close();

		}
		/*
		 * System.out.println("\ncode entry stat"); for (int ii = 20; ii > 0;
		 * ii--) { for (Entry<String, Integer> ee : code_stat.entrySet()) { if
		 * (ee.getValue() == ii) System.out.println("[" + ee.getKey() + "]:" +
		 * ee.getValue()); } }
		 * 
		 * System.out.println("\ncode -> onto"); for (Entry<String, String> ee :
		 * old_code__new_code.entrySet()) { System.out.println("[" + ee.getKey()
		 * + "]->[" + ee.getValue() + "]"); }
		 */
	}

	private static int count_documents_with_versions = 0;
	private static int max_count_versons_of_document = 0;

	private static IXMLParser parser = null;

	private static Map<String, String> prepared_docs = new HashMap<String, String>();

	private static void prepare_document(String docId, PacahonClient pacahon_client, String ticket) throws Exception
	{
		{
			if (prepared_docs.get(docId) != null)
				return;
			prepared_docs.put(docId, "+");
		}

		if (parser == null)
			parser = XMLParserFactory.createDefaultXMLParser();

		String docDataQuery = "select content, kindOf, timestamp, recordId, objectId FROM objects where objectId = '"
				+ docId + "' order by timestamp desc";
		Statement st1 = connection.createStatement();
		ResultSet docRecordRs = st1.executeQuery(docDataQuery);

		int count_of_version = 0;
		if (max_count_versons_of_document < count_of_version)
			max_count_versons_of_document = count_of_version;

		while (docRecordRs.next())
		{
			count_of_version++;

			String docXmlStr = docRecordRs.getString(1);
			Object timestamp = docRecordRs.getObject(3);
			String recordId = docRecordRs.getString(4);
			String id = docRecordRs.getString(5);

			try
			{
				IXMLElement xmlDoc;

				try
				{
					docXmlStr = docXmlStr.replace("\"", "*"); // @@@
					IXMLReader reader = StdXMLReader.stringReader(docXmlStr);
					parser.setReader(reader);
					xmlDoc = (IXMLElement) parser.parse(true);
					reader.close();
				} catch (Exception ex)
				{
					ex.printStackTrace(System.out);
					System.out.println("invalid document [" + docXmlStr + "]");

					continue;
				}

				String current_doc_id = null;

				String doc_id = null;

				String objectType = util.get(xmlDoc, "objectType", null);
				if (objectType.equals("DICTIONARY"))
				{
					doc_id = predicates.zdb + "dict_" + id;
				} else
				{
					doc_id = predicates.zdb + "doc_" + id;
				}
				current_doc_id = doc_id;

				if (timestamp != null)
				{
					if (objectType.equals("DICTIONARY"))
					{
						doc_id = predicates.zdb + "dict_" + id + "_v_" + count_of_version;
					} else
					{
						doc_id = predicates.zdb + "doc_" + id + "_v_" + count_of_version;
					}
				}
				recordId__versionId.put(recordId, doc_id);
				System.out.println("recordId = " + recordId + " -> docUri" + doc_id);
				// versionId__recordId.put(doc_id, recordId);

				{
					count_documents_with_versions++;

					if (count_documents_with_versions % 1000 == 0)
						System.out.println("count documents with versions = " + count_documents_with_versions
								+ ", unique docs = " + count_documents + ", max count versions of document="
								+ max_count_versons_of_document);
				}

				// id = util.get(xmlDoc, "id", null);
				System.out.println("* doc id = " + id);

				String authorId = util.get(xmlDoc, "authorId", null);
				String dateCreatedStr = util.get(xmlDoc, "dateCreated", null);

				if (dateCreatedStr == null)
					System.out.println("date create is null");

				Date date_created = null;

				if (dateCreatedStr != null)
					date_created = util.string2date(dateCreatedStr);

				String dateLastModified = util.get(xmlDoc, "dateLastModified", null);
				String lastEditorId = util.get(xmlDoc, "lastEditorId", null);
				String typeId = util.get(xmlDoc, "typeId", null);

				Model node = ModelFactory.createDefaultModel();
				node.setNsPrefixes(predicates.getPrefixs());

				Resource r = null;

				String tmplRcId[] = util.getRecordIdAndTemplateIdOfDocId__OnDate(typeId, date_created, connection);
				String templateId = recordId__versionId.get(tmplRcId[0]);

				docUri__templateUri.put(doc_id, templateId);

				r = node.createResource(doc_id);

				r.addProperty(ResourceFactory.createProperty(predicates.docs__document),
						ResourceFactory.createProperty(current_doc_id));

				if (timestamp == null)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.gost19__volatile),
							node.createLiteral("true"));
				}

				try
				{
					r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(templateId));
				} catch (Exception ex)
				{
					ex.printStackTrace(System.out);
					System.out.println("^templateId=[" + templateId + "], typeId=[" + typeId + "], tmplRcId=["
							+ tmplRcId + "]");
				}

				r.addProperty(ResourceFactory.createProperty(predicates.docs__document),
						ResourceFactory.createProperty(predicates.docs__Document));

				r.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
						ResourceFactory.createProperty(predicates.docs__Document));

				r.addProperty(ResourceFactory.createProperty(predicates.dc__identifier), node.createLiteral(id));

				if (dateCreatedStr != null)
				{
					r.addProperty(ResourceFactory.createProperty(predicates.swrc__creationDate),
							node.createLiteral(dateCreatedStr));
				}

				if (authorId != null)
					addOuToDocument(doc_id, authorId, predicates.dc__creator, node, r);

				Vector<IXMLElement> atts = null;
				atts = xmlDoc.getFirstChildNamed("xmlAttributes").getChildren();

				if (atts != null)
				{
					for (IXMLElement att_list_element : atts)
					{
						String type = util.get(att_list_element, "type", "");
						String att_name = util.get(att_list_element, "name", "");
						String att_code = util.get(att_list_element, "code", "");

						if (exclude_codes_doc.indexOf(att_name) >= 0)
						{
							// System.out.println("\n	att_name=[" + att_name +
							// "] is skipped");

							continue;
						}

						String onto_code = old_code__new_code.get(att_code);
						if (onto_code == null)
							onto_code = renameCodeToOnto(att_code, null, att_name);

						if (type.equals("ORGANIZATION"))
						{
							/*
							 * <xmlAttribute>
							 * <dateCreated>2010-02-02T09:40:08.862
							 * +03:00</dateCreated> <description></description>
							 * <multiSelect>true</multiSelect> <name>Кому</name>
							 * <obligatory>true</obligatory>
							 * <organizationTag>user</organizationTag>
							 * <organizationValue
							 * >fb926d69-3a49-4842-a2e2-e592fd301073
							 * </organizationValue> <type>ORGANIZATION</type>
							 * <computationalConfirm
							 * >NONE</computationalConfirm>
							 * <computationalReadonly
							 * >false</computationalReadonly> <code>Кому</code>
							 * <xmlAttributes/> </xmlAttribute>
							 */
							String organizationValue = util.get(att_list_element, "organizationValue", null);
							String organizationTag = util.get(att_list_element, "organizationTag", null);

							if (organizationValue != null)
							{
								addOuToDocument(doc_id, organizationValue, onto_code, node, r);
							}
						} else if (type.equals("TEXT") || type.equals("STRING"))
						{
							String textValue = util.get(att_list_element, "textValue", null);
							if (textValue != null && textValue.length() > 0)
							{
								r.addProperty(ResourceFactory.createProperty(onto_code), node.createLiteral(textValue));
							}
						} else if (type.equals("LINK"))
						{
							String value = util.get(att_list_element, "linkValue", null);

							if (value != null && value.length() > 0)
							{
								addLinkToDocument(doc_id, value, onto_code, node, r, pacahon_client, ticket,
										date_created);
							}

						} else if (type.equals("DICTIONARY"))
						{
							String value = util.get(att_list_element, "recordIdValue", null);

							if (value != null && value.length() > 0)
							{
								addLinkToDocument(doc_id, value, onto_code, node, r, pacahon_client, ticket,
										date_created);
							}

						} else if (type.equals("DATEINTERVAL"))
						{
							// dateFromValue
							// dateToValue
							String value = "";
							String value1 = util.get(att_list_element, "dateFromValue", null);
							String value2 = util.get(att_list_element, "dateToValue", null);

							if (value1 != null && value1.length() > 0)
							{
								value = value1;
							}
							value += ";";
							if (value1 != null && value1.length() > 0)
							{
								value += value2;
							}

							if (value != null && value.length() > 0)
							{
								r.addProperty(ResourceFactory.createProperty(onto_code), node.createLiteral(value));
							}
						} else if (type.equals("DATE"))
						{
							// dateValue
							String value = util.get(att_list_element, "dateValue", null);
							if (value != null && value.length() > 0)
							{
								r.addProperty(ResourceFactory.createProperty(onto_code), node.createLiteral(value));
							}
						} else if (type.equals("NUMBER"))
						{
							// numberFromValue
							String value = util.get(att_list_element, "numberValue", null);
							if (value != null && value.length() > 0)
							{
								r.addProperty(ResourceFactory.createProperty(onto_code), node.createLiteral(value));
							}
						} else if (type.equals("FILE"))
						{
							// fileValue
							type.hashCode();
						} else if (type.equals("BOOLEAN"))
						{
							// flagValue
							String value = util.get(att_list_element, "flagValue", null);
							if (value != null && value.length() > 0)
							{
								r.addProperty(ResourceFactory.createProperty(onto_code), node.createLiteral(value));
							}
						} else
						{
							type.hashCode();
						}

					}
				}

				pacahon_client.put(ticket, node);

			} catch (Exception ex)
			{
				ex.printStackTrace(System.out);
				System.out.println("skip document id=" + id + ", reson:" + ex.getMessage());
				System.out.println("document [" + docXmlStr + "]");
			}
		}
		docRecordRs.close();
		st1.close();

	}

	private static int count_documents = 0;

	private static int fetchDocuments(PacahonClient pacahon_client, String ticket) throws Exception
	{
		System.out.println("fetch documents");

		System.out.println("select all documents id's");
		String docsIdDataQuery = "select objectId " + "FROM objects " + "WHERE isTemplate = 0 and timestamp is null"; // and
																														// objectId
																														// =
																														// '6a9642f9806f40228b62add2930698b8'
																														// ";
		ResultSet docRecordsRs = connection.createStatement().executeQuery(docsIdDataQuery);
		System.out.println("prepare");

		while (docRecordsRs.next())
		{
			String docId = docRecordsRs.getString(1);
			count_documents++;
			prepare_document(docId, pacahon_client, ticket);
		}

		docRecordsRs.close();

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

			// System.out.print(deps);

			ArrayList<String> excludeNode = new ArrayList<String>();
			excludeNode.add("1154685117926");// 14dd8e2e-634f-4332-bc4d-4bc708a9ff64:1154685117926:_ТЕЛЕФОНЫ
												// СПЕЦВЫЗОВА
			// excludeNode.add("1146725963873");
			// excludeNode.add("1000");
			// excludeNode.add("123456");

			// находим родителей для всех подразделений
			int buCounter = 0;
			departmentsOfExtIdMap = new HashMap<String, Department>();
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

				Resource r_ou = null;
				Resource r = null;
				String ouId = null;

				if (isDepartment == true)
				{
					ouId = predicates.zdb + "dep_" + department.getId();

					System.out.println(ii + " add department " + department.getNameRu() + ", ouid=" + ouId);

					r_ou = node.createResource(ouId);
					r_ou.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.swrc__Department));

					if (department.isActive())
					{
						r_ou.addProperty(ResourceFactory.createProperty(predicates.docs__active),
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
							ResourceFactory.createProperty(predicates.zdb, "dep_" + department.getId()));

					r.addProperty(ResourceFactory.createProperty(predicates.swrc__organization),
							ResourceFactory.createProperty(predicates.zdb, "org_" + department.getOrganizationId()));

					r.addProperty(ResourceFactory.createProperty(predicates.gost19__externalIdentifer),
							node.createLiteral(department.getExtId()));

				} else if (isOrganization)
				{
					ouId = predicates.zdb + "org_" + department.getId();

					System.out.println(ii + " add organization " + department.getNameRu() + ", ouid=" + ouId);

					r_ou = node.createResource(ouId);
					r_ou.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.swrc__Organization));

					if (department.isActive())
					{
						r_ou.addProperty(ResourceFactory.createProperty(predicates.docs__active),
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
					ouId = predicates.zdb + "group_" + department.getId();

					System.out.println(ii + " add group " + department.getNameRu() + ", ouid=" + ouId);

					r_ou = node.createResource(ouId);
					r_ou.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
							ResourceFactory.createProperty(predicates.docs__Group));

					if (department.isActive())
					{
						r_ou.addProperty(ResourceFactory.createProperty(predicates.docs__active),
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

					write_add_info_of_attribute(predicates.zdb + "doc_" + department.getId(),
							predicates.docs__parentUnit, predicates.zdb + "dep_" + parent, predicates.swrc__name,
							departmentsOfExtIdMap.get(parent).getNameRu(), node);
				}

				ouUri__userObj.put(ouId, department);

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
				System.out.println(ii + " add user " + userId + ", oiud=" + predicates.zdb + "person_" + userId);

				ouUri__userObj.put(predicates.zdb + "person_" + userId, userEntity);

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
									ResourceFactory.createProperty(predicates.zdb, "dep_" + department.getId()));

							write_add_info_of_attribute(predicates.zdb + "doc_" + userId, predicates.docs__unit,
									predicates.zdb + "dep_" + department.getId(), predicates.swrc__name,
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

			e.printStackTrace(System.out);

			printUsage();

		}
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private static void addLinkToDocument(String docUri, String linkDocId, String attUri, Model node, Resource r,
			PacahonClient pacahon_client, String ticket, Date date_created) throws Exception
	{
		if (linkDocId == null || linkDocId.length() < 2)
			return;

		String rId[] = util.getRecordIdAndTemplateIdOfDocId__OnDate(linkDocId, date_created, connection);
		if (rId == null)
		{
			System.out.println("invalid linked doc detected: [" + linkDocId + "]");

			return;
		}

		String vId = recordId__versionId.get(rId[0]);

		if (vId == null)
		{
			System.out.println("		prepare for linked doc [" + linkDocId + "]");
			prepare_document(linkDocId, pacahon_client, ticket);
			vId = recordId__versionId.get(rId[0]);

			if (vId == null)
			{
				throw new Exception("!!!(^$%$^@^%$%@$*&%@%%@$%%$%@$^$@!$#$^%@$, linked doc:" + linkDocId);
			}
		}

		String linkDocUri = vId;
		String templateId = docUri__templateUri.get(linkDocUri);

		System.out.println("		linked doc:" + linkDocId);

		r.addProperty(ResourceFactory.createProperty(attUri), ResourceFactory.createProperty(linkDocUri));

		// возможно у этого поля есть собственные представления для линка на
		// документ
		// возьмем сначала его
		String[] def_repr = templateUri_fieldUri__takedUri.get(docUri + "+" + attUri);

		if (def_repr == null)
		{
			// у документа linkDocId нужно считать поля def_repr[]
			def_repr = templateId__defaultRepresentation.get(templateId);
			if (def_repr == null || def_repr.length == 0)
			{
				def_repr = new String[1];
				def_repr[0] = predicates.swrc__name;
			}
		}

		// и сохранить их здесь

		Model node1 = ModelFactory.createDefaultModel();
		node1.setNsPrefixes(predicates.getPrefixs());

		Resource r1 = node1.createResource(linkDocUri);

		for (String filed : def_repr)
		{
			try
			{
				r1.addProperty(ResourceFactory.createProperty(filed),
						ResourceFactory.createProperty(predicates.query__get));
			} catch (Exception ex)
			{
				ex.hashCode();
			}
		}

		Model mm = pacahon_client.get(ticket, node1);

		if (mm != null && mm.size() > 0)
		{
			StmtIterator it = mm.listStatements();

			while (it.hasNext())
			{
				com.hp.hpl.jena.rdf.model.Statement sss = it.nextStatement();

				System.out.println("			   		" + sss.getPredicate() + ", " + sss.getLiteral());

				write_add_info_of_attribute(docUri, attUri, linkDocUri, sss.getPredicate(), sss.getLiteral(), node);
			}

		} else
		{
			System.out.println("not found data for property [" + attUri + "] in document [" + linkDocUri + "], defrep:"
					+ def_repr.toString());
		}

	}

	private static void addOuToDocument(String docUri, String ouId, String attUri, Model node, Resource r)
			throws Exception
	{
		if (ouId == null || ouId.length() < 2)
			return;

		String ouUri = predicates.zdb + "person_" + ouId;
		String ouDocUri = predicates.zdb + "doc_" + ouId;
		Object qqq = ouUri__userObj.get(ouUri);

		if (qqq != null)
		{
			EntityType person = (EntityType) qqq;
			r.addProperty(ResourceFactory.createProperty(attUri), ResourceFactory.createProperty(ouUri));

			write_add_info_of_attribute(docUri, attUri, ouUri, ResourceFactory.createProperty(predicates.docs__source),
					ResourceFactory.createProperty(ouDocUri), node);

			for (AttributeType a : person.getAttributes().getAttributeList())
			{
				String name = a.getName();
				String value = a.getValue();
				if (value != null && value.length() > 0)
				{
					if (name.equalsIgnoreCase("firstNameRu"))
					{
						write_add_info_of_attribute(docUri, attUri, ouUri, predicates.swrc__firstName, value, node);

					} else if (name.equalsIgnoreCase("surnameRu"))
					{
						write_add_info_of_attribute(docUri, attUri, ouUri, predicates.swrc__lastName, value, node);
					} else if (name.equalsIgnoreCase("secondnameRu"))
					{
						write_add_info_of_attribute(docUri, attUri, ouUri, predicates.gost19__middleName, value, node);
					} else if (name.equalsIgnoreCase("postRu"))
					{
						write_add_info_of_attribute(docUri, attUri, ouUri, predicates.docs__position, value, node);
					} else if (name.equalsIgnoreCase("departmentId"))
					{
						Department department = departmentsOfExtIdMap.get(value);
						write_add_info_of_attribute(docUri, attUri, ouUri, predicates.swrc__name,
								department.getNameRu(), node);

					}
				}
			}
		} else if (qqq == null)
		{
			ouUri = predicates.zdb + "dep_" + ouId;
			qqq = ouUri__userObj.get(ouUri);
		}

		if (qqq == null)
		{
			ouUri = predicates.zdb + "org_" + ouId;
			qqq = ouUri__userObj.get(ouUri);
		}

		if (qqq == null)
		{
			ouUri = predicates.zdb + "group_" + ouId;
			qqq = ouUri__userObj.get(ouUri);
		}

		if (qqq == null)
			throw new Exception("user [" + ouId + "] not found");

	}

	private static void write_add_info_of_attribute(String subject, String predicate, String object,
			String addInfo_predicate, String addInfo_value, Model node) throws Exception
	{
		String addinfo_subject = predicates.gost19 + "add_info_" + subject.hashCode() + "" + predicate.hashCode() + ""
				+ object.hashCode();

		Resource r_department = node.createResource(addinfo_subject);

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
				ResourceFactory.createProperty(predicates.rdf, "Statement"));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__subject),
				ResourceFactory.createProperty(subject));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__predicate),
				ResourceFactory.createProperty(predicate));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__object),
				ResourceFactory.createProperty(object));

		r_department.addProperty(ResourceFactory.createProperty(addInfo_predicate), node.createLiteral(addInfo_value));
	}

	private static void write_add_info_of_attribute(String subject, String predicate, String object,
			Property addInfo_predicate, Literal addInfo_value, Model node) throws Exception
	{
		String addinfo_subject = predicates.gost19 + "add_info_" + subject.hashCode() + "" + predicate.hashCode() + ""
				+ object.hashCode();

		Resource r_department = node.createResource(addinfo_subject);

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
				ResourceFactory.createProperty(predicates.rdf, "Statement"));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__subject),
				ResourceFactory.createProperty(subject));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__predicate),
				ResourceFactory.createProperty(predicate));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__object),
				ResourceFactory.createProperty(object));

		r_department.addProperty(addInfo_predicate, addInfo_value);
	}

	private static void write_add_info_of_attribute(String subject, String predicate, String object,
			Property addInfo_predicate, Property addInfo_value, Model node) throws Exception
	{
		String addinfo_subject = predicates.gost19 + "add_info_" + subject.hashCode() + "" + predicate.hashCode() + ""
				+ object.hashCode();

		Resource r_department = node.createResource(addinfo_subject);

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__type),
				ResourceFactory.createProperty(predicates.rdf, "Statement"));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__subject),
				ResourceFactory.createProperty(subject));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__predicate),
				ResourceFactory.createProperty(predicate));

		r_department.addProperty(ResourceFactory.createProperty(predicates.rdf__object),
				ResourceFactory.createProperty(object));

		r_department.addProperty(addInfo_predicate, addInfo_value);
	}

	private static String renameCodeToOnto(String _code, String id, String alternativeName)
	{
		String code = _code;
		String this_code_in_onto = code_onto.get(code.toLowerCase());

		// if (code.equals("3"))
		// System.out.println("?");

		if (this_code_in_onto == null)
		{
			if ((code.indexOf('1') >= 0 || code.indexOf('2') >= 0 || code.indexOf('3') >= 0 || code.indexOf('4') >= 0
					|| code.indexOf('5') >= 0 || code.indexOf('6') >= 0 || code.indexOf('7') >= 0
					|| code.indexOf('8') >= 0 || code.indexOf('9') >= 0 || code.indexOf('0') >= 0)
					&& alternativeName != null)
			{
				// если code содержит uid, то заменим code на
				// название из метки
				if (id != null && id.equals("0027562cbe0948e5965c3183eb23e42c") == false)
					this_code_in_onto = alternativeName;
			}

			try
			{
				String ff = code.charAt(0) + "";
				int qq = Integer.parseInt(ff);
				if (qq > 0)
				{

				}
				code = "z" + code;
			} catch (Exception ex)
			{
				ex.hashCode();
			}

			if (code.length() == 0)
				code = alternativeName;

			this_code_in_onto = predicates.user_onto
					+ Translit.toTranslit(code).replace('/', '_').replace(' ', '_').replace('\'', '_')
							.replace('№', 'N').replace(',', '_').replace('(', '_').replace(')', '_').replace('.', '_')
							.replace('-', '_').replace('%', 'P').toLowerCase();

		}

		old_code__new_code.put(_code, this_code_in_onto);

		// if (this_code_in_onto == null || this_code_in_onto.equals("null"))
		// System.out.println("?");

		return this_code_in_onto;
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	static void init_source() throws Exception
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

			documentTypeId = properties.getProperty("documentTypeId", "");
			ticketId = properties.getProperty("sessionTicketId", "");
			// SEARCH_URL = properties.getProperty("searchUrl", "");
			// DOCUMENT_SERVICE_URL = properties.getProperty("documentsUrl",
			// "");
			// fake = new Boolean(properties.getProperty("fake", "false"));
			pathToDump = properties.getProperty("pathToDump");
			dbUser = properties.getProperty("dbUser", "ba");
			dbPassword = properties.getProperty("dbPassword", "123456");
			dbUrl = properties.getProperty("dbUrl", "localhost:3306");
			// dbSuffix = properties.getProperty("dbSuffix", "");
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
			code_onto.put("date_to", predicates.swrc__endDate);
			code_onto.put("file", predicates.docs__FileDescription);
			code_onto.put("from", predicates.docs__from);
			code_onto.put("name", predicates.swrc__name);
			code_onto.put("subject/тема", predicates.dc__subject);
			code_onto.put("to", predicates.swrc__endDate);
			code_onto.put("в копию", predicates.docs__carbon_copy);
			code_onto.put("вложение", predicates.docs__FileDescription);
			code_onto.put("вложения", predicates.docs__FileDescription);
			code_onto.put("дата начала", predicates.swrc__startDate);
			code_onto.put("дата окончания", predicates.swrc__endDate);
			code_onto.put("дата окончания (планируемая)", predicates.swrc__endDate);
			code_onto.put("заголовок", predicates.dc__title);
			code_onto.put("ключевые слова", predicates.swrc__keywords);
			code_onto.put("комментарии", predicates.swrc__note);
			code_onto.put("комментарий", predicates.swrc__note);
			code_onto.put("кому", predicates.docs__to);
			code_onto.put("контрагент", predicates.docs__contractor);
			code_onto.put("контрагент (название/ страна/ город)", predicates.docs__contractor);
			code_onto.put("краткое содержание", predicates.dc__description);
			code_onto.put("название", predicates.swrc__name);
			code_onto.put("наименование", predicates.swrc__name);
			code_onto.put("номер", predicates.swrc__number);
			code_onto.put("от кого", predicates.docs__from);
			code_onto.put("подразделение", predicates.docs__unit);
			code_onto.put("полное название", predicates.swrc__name);
			code_onto.put("содержание", predicates.docs__content);
			code_onto.put("ссылка на документ", predicates.docs__link);
			code_onto.put("тема", predicates.dc__subject);
			code_onto.put("тип", predicates.dc__type);

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

		fetchOrganization(pacahon_client, ticket);
		fetchDocumentTypes(pacahon_client, ticket);

		Iterator<Entry<String, String>> it = old_code__new_code.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<String, String> e = it.next();
			System.out.println(e.getKey() + " => " + e.getValue());
		}

		fetchDocuments(pacahon_client, ticket);

		System.out.println("end... sleep...");

		Thread.currentThread().sleep(999999999);
	}
}
