package magnetico.objects.organization;

import java.io.Serializable;

import magnetico.ws.organization.AttributeType;
import magnetico.ws.organization.EntityType;

/**
 * 
 * @author SheringaA
 */
public class Department implements Serializable
{

	private static final long serialVersionUID = 1;
	private String nameEn;
	private String nameRu;
	private String id;
	private String extId;
	private String organizationId;
	private boolean isActive = false;
	public boolean doNotSyncronize = false;

	public Department()
	{
	}

	public Department(EntityType blObject, String locale)
	{
		setId(blObject.getUid());

		if (blObject.getAttributes() != null)
		{
			for (AttributeType a : blObject.getAttributes().getAttributeList())
			{
				if (a.getName().equalsIgnoreCase("nameRu"))
				{
					setNameRu(a.getValue());
				} // end else if
				if (a.getName().equalsIgnoreCase("nameEn"))
				{
					setNameEn(a.getValue());
				} // end else if
				else if (a.getName().equalsIgnoreCase("id"))
				{
					setExtId(a.getValue());
				} // end else if
				else if (a.getName().equalsIgnoreCase("active"))
				{
					if (a.getValue().equals("true"))
						setActive(true);
				} else if (a.getName().equalsIgnoreCase("doNotSynchronize"))
				{
					if (a.getValue().equals("1"))
						doNotSyncronize = true;
				}
				// end else if
			}
		} // end create_department()
	}

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public String getExtId()
	{
		return extId;
	}

	public void setExtId(String internalId)
	{
		this.extId = internalId;
	}

	public String getOrganizationId()
	{
		return organizationId;
	}

	public void setOrganizationId(String organizationId)
	{
		this.organizationId = organizationId;
	}

	public void setActive(boolean isActive)
	{
		this.isActive = isActive;
	}

	public boolean isActive()
	{
		return isActive;
	}

	public void setNameEn(String nameEn)
	{
		this.nameEn = nameEn;
	}

	public String getNameEn()
	{
		return nameEn;
	}

	public void setNameRu(String nameRu)
	{
		this.nameRu = nameRu;
	}

	public String getNameRu()
	{
		return nameRu;
	}

	public String toString()
	{
		return id + ":" + extId + ":" + nameRu + "\n";
	}
}
