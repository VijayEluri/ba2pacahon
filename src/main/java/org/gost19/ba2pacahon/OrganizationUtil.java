package org.gost19.ba2pacahon;

//   name="OrganizationEntityService"
//	 namespace="http://organization.magnet.magnetosoft.ru/"
//	 url="http://@organization.embedded-http-server.address@:@organization.embedded-http-server.port@/organization/OrganizationEntitySvc?wsdl"

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import magnetico.ws.organization.AdjacencyTypeEnum;
import magnetico.ws.organization.EntityContainerType;
import magnetico.ws.organization.EntityType;
import magnetico.ws.organization.OrganizationEntityEndpoint;
import magnetico.ws.organization.OrganizationEntityService;
import magnetico.ws.organization.OrganizationServiceException_Exception;
import magnetico.ws.organization.ParameterType;
import magnetico.ws.organization.PreparedQueryType;
import magnetico.ws.organization.PreparedQueryType.Parameters;
import magnetico.objects.organization.Department;

public class OrganizationUtil {

	private OrganizationEntityEndpoint organizationInvoker;
	private String locale = "Ru";
	private static final String USER_DEPARTMENT_RELATION = "contact-department";

	private OrganizationUtil() 
	{
	}

	public OrganizationUtil(String url, String namespace, String name) {
		try {
			organizationInvoker = new OrganizationEntityService(new URL(url),
					new QName(namespace, name))
					.getOrganizationEntityServiceEndpointPort();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	public List<Department> getDepartments() {

		List<Department> out = new ArrayList<Department>();

		if (organizationInvoker != null) {
			try {

				PreparedQueryType query = createQuery("*", "name" + locale
						+ " like^ ?", "*", "*");
				EntityContainerType ec = organizationInvoker
						.queryEntityContainer("department", query);

				if (ec != null) {
					for (EntityType blo : ec.getEntities().getEntityList()) {
						out.add(new Department(blo, locale));
					} // end for
				}

			} catch (OrganizationServiceException_Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return out;
	}

	public EntityType getUser (String userId) throws Exception
	{
		return organizationInvoker.getEntity(userId);
	}
	
	public List<EntityType> getUsers() {

		List<EntityType> result = null;
		try {
			String suffix = firstLetterUp(locale);
			PreparedQueryType query = createQuery(
					"*",
					"",
					"", "*");

			result = organizationInvoker.queryEntityContainer("contact", query)
					.getEntities().getEntityList();
			// if (ec.getEntities() != null) {
			// for (EntityType blo : ec.getEntities().getEntityList()) {
			// User u = new User(blo, locale);
			// u.setDepartment(getDepartmentByUserId(u.getId(), locale));
			// result.add(u);
			// } // end for
			// }

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public void setLocale(String locale) {
		this.locale = firstLetterUp(locale);
	}

	protected String firstLetterUp(String str) {
		if (str == null || "".equals(str)) {
			return str;
		}
		return str.substring(0, 1).toUpperCase() + str.substring(1);
	}

	protected PreparedQueryType createQuery(String tokens, String mainClause,
			String valuePrefix, String valueSuffix) {
		PreparedQueryType query = new PreparedQueryType();
		Parameters params = new Parameters();

		StringBuilder clause = null;
		// to search with "?"
		tokens = tokens.replace("?", "*");
		List<String> input = StringFormat.string2List(tokens, ' ');
		for (String val : input) {
			if (clause == null) {
				clause = new StringBuilder();
			} else {
				clause.append(" and ");
			}
			clause.append(mainClause);
			int begin = mainClause.indexOf("?");
			while (begin >= 0) {
				ParameterType pt = new ParameterType();
				pt.setValue(valuePrefix + val + valueSuffix);
				params.getParameterList().add(pt);
				begin = mainClause.indexOf("?", begin + 1);
			}
		}

		query.setParameters(params);
		if (clause != null) {
			query.setWhereClause(clause.toString());
		}
		return query;
	}

	public Department getDepartmentByUserId(String userId, String locale) {

		Department result = null;
		try {
			List<String> deptIds = organizationInvoker.getAdjacency(userId,
					USER_DEPARTMENT_RELATION, AdjacencyTypeEnum.FORWARD);

			if (deptIds.size() == 1) {
				EntityType et = organizationInvoker.getEntity(deptIds.get(0));
				result = new Department(et, locale);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public List<Department> getDepartmentsByParentId(String parentId,
			String locale) {
		List<Department> result = new ArrayList<Department>();
		try {

			PreparedQueryType query = new PreparedQueryType();
			query.setWhereClause("parentId = '" + parentId + "'");

			List<String> uids = organizationInvoker.queryUids("department",
					query);
			if (!uids.isEmpty()) {
				List<EntityType> entities = organizationInvoker
						.getEntities(uids);
				for (EntityType blo : entities) {
					Department d = new Department(blo, locale);
					result.add(d);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

}
