@XmlSchema(
        namespace = "http://sap.com/xi/SAPGlobal20/Global",
        elementFormDefault = XmlNsForm.QUALIFIED,
        xmlns = {
                @XmlNs(prefix = "glob", namespaceURI = "http://sap.com/xi/SAPGlobal20/Global")
        }
)
package com.hedno.integration.soap.model;

import javax.xml.bind.annotation.XmlNs;
import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.XmlSchema;
