package magnetico.objects.organization;

import java.io.Serializable;

import magnetico.ws.organization.AttributeType;
import magnetico.ws.organization.EntityType;


/**
 * 
 * @author SheringaA
 */
public class Department implements Serializable {

    private static final long serialVersionUID = 1;
    private String name;
    private String id;
    private String extId;
    private String organizationId;

    public Department() {
    }

    public Department(EntityType blObject, String locale) {
        setId(blObject.getUid());

        if (blObject.getAttributes() != null) {
            for (AttributeType a : blObject.getAttributes().getAttributeList()) {
                if (a.getName().equalsIgnoreCase("name" + locale)) {
                    setName(a.getValue());
                } // end else if
                else if (a.getName().equalsIgnoreCase("id")) {
                    setExtId(a.getValue());
                } // end else if
            }
        } // end create_department()
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExtId() {
        return extId;
    }

    public void setExtId(String internalId) {
        this.extId = internalId;
    }
    
    public String getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }
}
