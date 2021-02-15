/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.syntez.processors.router.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import ru.syntez.processors.router.processor.entities.OutputDocumentExt;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example", "transform", "custom", "demo"})
@CapabilityDescription("Example custom processor to route document by routing key on header")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RouterProcessor extends AbstractProcessor {

    public static final Relationship REL_ORDER_SUCCESS = new Relationship.Builder()
            .name("REL_ORDER_SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship REL_INVOICE_REPLAY = new Relationship.Builder()
            .name("REL_INVOICE_REPLAY")
            .description("Replay relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("REL_FAILURE")
            .description("Failture relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private JacksonXmlModule xmlModule = new JacksonXmlModule();
    private ObjectMapper xmlMapper = new XmlMapper(xmlModule);

    private void initXMLMapper() {
        xmlModule.setDefaultUseWrapper(false);
        ((XmlMapper) xmlMapper).enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORDER_SUCCESS);
        relationships.add(REL_INVOICE_REPLAY);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        initXMLMapper();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if ( inputFlowFile == null ) {
            return;
        }

        final OutputDocumentExt outputDocumentExt;
        try (InputStream inputStream = session.read(inputFlowFile)) {
            outputDocumentExt = xmlMapper.readValue(inputStream, OutputDocumentExt.class);
        } catch (Exception ex) {
            getLogger().error("Failed to read XML string: " + ex.getMessage());
            session.write(inputFlowFile);
            session.transfer(inputFlowFile, REL_FAILURE);
            return;
        }

        FlowFile routeFlowFile = session.create(inputFlowFile);
        session.write(routeFlowFile, out -> out.write(xmlMapper.writeValueAsBytes(outputDocumentExt)));

        //getLogger().warn("RoutingKey: " + inputFlowFile.getAttribute("RoutingKey"));

        if (inputFlowFile.getAttribute("RoutingKey").equals("order")) {
            session.remove(inputFlowFile);
            session.transfer(routeFlowFile, REL_ORDER_SUCCESS);
        } else if (inputFlowFile.getAttribute("RoutingKey").equals("invoice")) {
            session.remove(inputFlowFile);
            session.transfer(routeFlowFile, REL_INVOICE_REPLAY);
        } else {
            session.write(inputFlowFile);
            session.transfer(inputFlowFile, REL_FAILURE);
        }

    }
}
