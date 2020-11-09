package com.linuxforhealth.connect.builder;

import java.time.Instant;
import java.util.Base64;
import java.util.Date;

import javax.ws.rs.HttpMethod;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.support.builder.PredicateBuilder;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaFhirConsumerBuilder extends RouteBuilder {
	
	private final Logger logger = LoggerFactory.getLogger(KafkaFhirConsumerBuilder.class);
	final static String PROP_PATIENT_ID = "patientId";
	final static String PROP_ID = "id";
	final static String PROP_RESOURCE_TYPE = "resourceType";
	
	//
	// Kafka topic envelope for persisting nlp service response
	//
	static class NlpMessageEnvelope {
		
		String patientId;
		String data;
		String id;
		String resourceType;
		String resourceTypeElement;
		String serviceName;
		Long timestamp;
		String nlpModelId;
		
		public String getPatientId() {
			return patientId;
		}
		public void setPatientId(String patientId) {
			this.patientId = patientId;
		}
		public String getData() {
			return data;
		}
		public void setData(String data) {
			this.data = data;
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getResourceType() {
			return resourceType;
		}
		public void setResourceType(String resourceType) {
			this.resourceType = resourceType;
		}
		public String getResourceTypeElement() {
			return resourceTypeElement;
		}
		public void setResourceTypeElement(String resourceTypeElement) {
			this.resourceTypeElement = resourceTypeElement;
		}
		public String getServiceName() {
			return serviceName;
		}
		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}
		public String getNlpModelId() {
			return nlpModelId;
		}
		public void setNlpModelId(String nlpModelId) {
			this.nlpModelId = nlpModelId;
		}
		public Long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}
		
	}

	@Override
	public void configure() throws Exception {
		
		getContext().setStreamCaching(true); // Prevent exchange message body from disappearing after reads
		
		//
		// Consume topic messages from stream
		// INPUT:  LinuxForHealth Message Envelope
		// OUTPUT: FHIR R4 Resource
		//
		from("kafka:FHIR-R4_DOCUMENTREFERENCE,FHIR-R4_DIAGNOSTICREPORT,FHIR-R4_BUNDLE?brokers=localhost:9094")
			.log(LoggingLevel.DEBUG, logger, "[kafka-input]:\n ${body.substring(0,200)}")
		
			.setProperty("dataFormat").jsonpath("meta.dataFormat")
			
			.choice()
			
				.when(exchangeProperty("dataFormat").isEqualTo("FHIR-R4"))
					.split().jsonpath("data", true)
					.unmarshal().base64().convertBodyTo(String.class)

					.setProperty(PROP_RESOURCE_TYPE).jsonpath("resourceType", true)
				
					.choice()
					
						.when(exchangeProperty(PROP_RESOURCE_TYPE).isEqualTo("Bundle"))
							.split().jsonpath("entry[*].resource")
							.marshal().json()
							.to("direct:fhir-resource")
						.endChoice()
						
					.end()
					
					.to("direct:fhir-resource")
					
				.endChoice()
				
			.end()				
		;
		
		
		//
		// Process individual FHIR R4 resources
		// INPUT:  FHIR R4 Resource
		// OUTPUT: FHIR R4 Resource (routed appropriately)
		//
		from("direct:fhir-resource")
			.convertBodyTo(String.class)
			.log(LoggingLevel.DEBUG, logger, "[fhir-resource] INPUT:\n${body.substring(0,200)}")
	
			.setProperty(PROP_RESOURCE_TYPE).jsonpath("resourceType", true)
			.setProperty(PROP_PATIENT_ID).jsonpath("subject.reference", true)
			.setProperty(PROP_ID).jsonpath("id", true)

			.choice()
			
				.when(exchangeProperty(PROP_RESOURCE_TYPE).isEqualTo("DocumentReference"))		
					.multicast()
					.parallelProcessing()
					.to("direct:text-div", "direct:documentreference-attachment")
				.endChoice()
				
				.when(exchangeProperty(PROP_RESOURCE_TYPE).isEqualTo("DiagnosticReport"))
					.multicast()
					.parallelProcessing()
					.to("direct:text-div", "direct:diagnosticreport-attachment")
				.endChoice()
				
				.otherwise()
					.to("direct:text-div")
				.endChoice()
		
			.end()
		;
		
		
		//
		// Extract text from FHIR R4 resource Narrative (text.div)
		// INPUT:  FHIR R4 Resource, will check for and extract text.div elements
		// OUTPUT: Unstructured text (nlp-ready)
		//
		from("direct:text-div")
			.log(LoggingLevel.DEBUG, logger, "[text-div] INPUT:\n${body.substring(0,200)}")
			.setProperty("resourceTypeElement", constant("narrative"))
			
			.choice()
				.when().jsonpath("text.div", true)

					// << CAMEL-15769 Jira issue opened
					.split(jsonpath("text.div").tokenize("@@@"))
		
					.log(LoggingLevel.DEBUG, logger, "[text-div] before tika:\n${body}")
					// extract text from xhtml tags (e.g. <div>)
					.to("tika:parse?tikaParseOutputFormat=text") 
					.log(LoggingLevel.DEBUG, logger, "[text-div] after tika:\n${body}")
					
					.to("direct:nlp")
					
				.endChoice()
			.end()
		;
		
		
		//
		// Extract DiagnosticReport attachment
		// INPUT:  FHIR R4 DiagnosticReport Resource
		// OUTPUT: Attachment
		//
		from("direct:diagnosticreport-attachment")
			.choice()
				.when().jsonpath("presentedForm", true)
					.split().jsonpath("presentedForm", true)
					.to("direct:attachment")
				.endChoice()
			.end()
		;
		
		
		//
		// Extract DocumentReference attachments
		// INPUT:  FHIR R4 DocumentReference Resource
		// OUTPUT: Attachment
		//
		from("direct:documentreference-attachment")
			.choice()
				.when().jsonpath("content[*].attachment", true)
					.split().jsonpath("content[*].attachment", true)
					.to("direct:attachment")
				.endChoice()
			.end()
		;
		
		
		//
		// Extract text from FHIR R4 resource attachments
		// INPUT:  Attachment
		// OUTPUT: Unstructured text (nlp-ready)
		//
		from("direct:attachment")
		
			.setProperty("contentType").jsonpath("contentType", true)
			.setProperty("resourceTypeElement", constant("attachment"))
			.split().jsonpath("data", true)
			.unmarshal().base64()
			
			.choice()
			
				.when(PredicateBuilder.or(
					exchangeProperty("contentType").contains("application/pdf"), 
					exchangeProperty("contentType").contains("text/html")))
						.log(LoggingLevel.DEBUG, logger, "[attachment] before tika:\n${body}")
						// Convert PDF message to Text
						.to("tika:parse?tikaParseOutputFormat=text")
						.log(LoggingLevel.DEBUG, logger, "[attachment] after tika:\n${body}")
						.to("direct:nlp")
				.endChoice()
				
				.when(exchangeProperty("contentType").contains("text/plain"))
					.to("direct:nlp")
				.endChoice()
				
			.end()
		;
		
		
		//
		// Route text through an NLP service + post response to Kafka
		// INPUT:  Unstructured text
		// OUTPUT: NLP Service Response
		//
		from("direct:nlp")

			.choice()
				.when(body().isNotNull())
		
					.setHeader(Exchange.HTTP_METHOD, constant(HttpMethod.POST))
					.setHeader(Exchange.CONTENT_TYPE, constant(ContentType.TEXT_PLAIN))
					.convertBodyTo(String.class)
					
					// NLP service http request
					// Can be modified to call other NLP rest apis
					.to("https://us-east.wh-acd.cloud.ibm.com/wh-acd/api/v1/analyze/"
							+ "wh_acd.ibm_clinical_insights_v1.0_standard_flow"
							+ "?version=2020-10-22"
							+ "&authMethod=Basic"
							+ "&authUsername={{lfh.connect.nlp.id}}"
							+ "&authPassword={{lfh.connect.nlp.apikey}}")
		
					.log(LoggingLevel.DEBUG, logger, "NLP Service Return Code: ${header.CamelHttpResponseCode}")
					
					.choice()
					
						.when(header("CamelHttpResponseCode").isEqualTo("200")) // successful response
							
							.process(new Processor() {
								@Override
								public void process(Exchange exchange) throws Exception {
									
									NlpMessageEnvelope envelope = new NlpMessageEnvelope();
									envelope.setData(Base64.getEncoder().encodeToString(
											exchange.getIn().getBody(String.class).getBytes()));
									envelope.setPatientId(exchange.getProperty(PROP_PATIENT_ID, String.class));
									envelope.setResourceType(exchange.getProperty(
											PROP_RESOURCE_TYPE, String.class));
									envelope.setId(exchange.getProperty(PROP_ID, String.class));
									envelope.setTimestamp(Instant.now().getEpochSecond());
									envelope.setResourceTypeElement(
											exchange.getProperty("resourceTypeElement", String.class));
									
									// Capture metadata regarding the NLP service employed
									envelope.setNlpModelId("wh_acd.ibm_clinical_insights_v1.0_standard_flow");
									envelope.setServiceName("ACD");
									
									exchange.getIn().setBody(envelope);
								}
							})
							
							.marshal().json(JsonLibrary.Jackson)
							.to("kafka:NLP_OUTPUT?brokers=localhost:9094") // publish kafka topic
						
						.endChoice()
						
					.end()
			.end()			
		;
		
		
		//
		// Consume NLP output off Kafka topic for processing
		// INPUT:  KafkaMessageEnvelope w/ embedded nlp service response
		// OUTPUT: Annotations from nlp service response
		//
		from("kafka:NLP_OUTPUT?brokers=localhost:9094")
			.log(LoggingLevel.DEBUG, logger, "[kafka-nlp] INPUT:\n${body.substring(0,200)}")

			.setProperty(PROP_PATIENT_ID).jsonpath("patientId")
			.split().jsonpath("data", true)
			.unmarshal().base64().convertBodyTo(String.class)
			
			.split().jsonpath("unstructured[*].data.*.[?(@.type && @.disambiguationData.validity != 'INVALID')]", true) // ACD specific jsonpath
			
			.to("direct:annotation")
		;
		
		
		//
		// Print nlp service annotation details
		// INPUT:  Annotations returned from nlp service
		// OUTPUT: Log annotations
		//
		from("direct:annotation")
			.marshal().json(true)
			.log(LoggingLevel.DEBUG, logger, "\n------------------------------------------\n${body}")
		;
		
		
	}

}