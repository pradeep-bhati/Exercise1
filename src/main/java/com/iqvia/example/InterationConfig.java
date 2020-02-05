package com.iqvia.example;

import java.io.File;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.logging.ConsoleHandler;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.hibernate.validator.internal.util.logging.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.logging.java.SimpleFormatter;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.config.xml.LoggingChannelAdapterParser;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.feed.dsl.Feed;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.dsl.FileWritingMessageHandlerSpec;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import com.rometools.rome.feed.synd.SyndEntry;

@Configuration
@EnableBinding(value = {Source.class,OutputProcessor.class})
public class InterationConfig {
	
	@Value("${feed.url}")
	private Resource feedResource;
	
	@Value("${root.folder}")
	private String rootFolder;
	
	@Autowired
	@Qualifier("errorparkchannel")
	private MessageChannel errorparkchannel;
	

	
	   @Bean
	   public IntegrationFlow feedFlow() {
	      return IntegrationFlows
	            .from(Feed.inboundAdapter(this.feedResource, "pubDate")
	                        .metadataStore(metadataStore()),
	                  e -> e.poller(p -> p.fixedDelay(100)))
	            .channel(MessageChannels.executor("executorChannel",threadPoolTaskExecutor()))
//	          .transform(Transformers.objectToString())
	           .enrichHeaders(h -> h.headerExpression("fileName","payload.uri.toString()"))
	           .enrichHeaders(h -> h.headerFunction("dir", m -> parseDirectory(m)))
	           .enrichHeaders(h -> h.headerFunction("logData", m -> logData(m)))
//	           .log(LoggingHandler.Level.INFO,"TEST_LOGGER",m -> m.getHeaders().get("logData"))
	           .transform(m -> transformSyndEntryToXmlString(m)) 
	           
//	           .handle(Files.outboundAdapter(m -> m.getHeaders().get("dir"))
//                       .fileNameGenerator(m -> m.getHeaders().get("fileName").toString()+".xml")
//                       .autoCreateDirectory(true))
//	           .channel(Source.OUTPUT)
	           .handle(getHandler())
	           .transform(m -> m.toString())
	           .log(LoggingHandler.Level.INFO,"TEST_LOGGER",m -> m.getHeaders().get("logData"))
//	           .logAndReply(LoggingHandler.Level.INFO,"TEST_LOGGER",m -> m.getHeaders().get("logData"));
//	            .channel(c -> c.queue("entries"))	          	           
	            .get();
	   }
	   
	   @Bean
	   public FileWritingMessageHandlerSpec getHandler()
	   {
		   FileWritingMessageHandlerSpec fileWritingMessageHandlerSpec =
		   (Files.outboundAdapter(m -> m.getHeaders().get("dir"))
           .fileNameGenerator(m -> m.getHeaders().get("fileName").toString()+".xml")
           .autoCreateDirectory(false));	   	
		   return fileWritingMessageHandlerSpec;
	   }
	   
//	   @InboundChannelAdapter(channel = Source.OUTPUT, poller = @Poller(fixedRate = "10000") )
//		public Message<?> generate(@Value("replyChannel") String xml ) {
//		   return MessageBuilder.withPayload(xml).build();
//	   }  
	   
	   @Bean
	   public TaskExecutor threadPoolTaskExecutor() {
	     
	     ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
	     executor.setThreadNamePrefix("Demo_Thread_");
	     executor.setMaxPoolSize(5);
	     executor.setCorePoolSize(5);
	     executor.setQueueCapacity(22);
	     return executor;
	   }
	   
	   String logData(Message<Object> m){
	        SyndEntry message = (SyndEntry) m.getPayload();
	        return message.getLink();
	   }
	
	   String parseDirectory(Message<Object> m){
	        SyndEntry message = (SyndEntry) m.getPayload();
	        String direcoryStructure = createDirectoryStructure(message);
	        return direcoryStructure;       
	    }
	   
	   String createDirectoryStructure(SyndEntry message) {
		   if(message.getPublishedDate()!=null)
	        {
	        	SimpleDateFormat smpl = new SimpleDateFormat("yyyy-MM-dd");
	        	String date = smpl.format(message.getPublishedDate());
	        	if(message.getCategories()!=null) {
	        		String category = message.getCategories().get(0).getName();
	        		return rootFolder+"/"+date+"/"+category;
	        	}
	        	else {
	        	return rootFolder+"/"+date+"categoryNotPresent";
	        	}
	        }
	        else
	        	{return rootFolder+"/"+"dateNotThere";}
		   
	   }

	
	 @Bean
	   public MetadataStore metadataStore() {
	        PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
	        metadataStore.setBaseDirectory("C:\\Users\\pradeep.bhati\\tmp\\foo");
	        return metadataStore;
	    }
	 
	   @Transformer(outputChannel = Source.OUTPUT)
	   String transformSyndEntryToXmlString(Object o)  {
		   System.out.println("bhati");
		  SyndEntry message = (SyndEntry) o;
		  Item item = populateItem(message);
		  String ItemXml = doMarshall(item);
		  return ItemXml;	             
	   }
	   
	   @Transformer(inputChannel = "replyChannel",outputChannel=Source.OUTPUT)
		public Message<String> generate(Message<String> message) { 
		   System.out.println("singh");
		   return MessageBuilder.withPayload("message1").build();
	   } 
	   
	   
	   
	   Item populateItem(SyndEntry message)
	   {
		   	Item item = new Item();
			item.setLink(message.getLink());
			item.setTitle(message.getTitle());
			if(message.getCategories()!=null) {
	      		String category = message.getCategories().get(0).getName();
	      		item.setCategory(category);
	      	}
			if(message.getPublishedDate()!=null) {
			  item.setPubDate(message.getPublishedDate().toString());
			}
			if(message.getDescription()!=null) {
			  item.setDescription(message.getDescription().getValue());
			}
			item.setGuid(message.getUri());
			item.setComment(message.getComments());
			return item;
	   }
	   
	   
	   String doMarshall(Item item) {
		   JAXBContext contextObj;
		   StringWriter sw = new StringWriter();
			try {
				contextObj = JAXBContext.newInstance(Item.class);
				Marshaller marshallerObj =  contextObj.createMarshaller();
				marshallerObj.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true); 
				marshallerObj.marshal(item, sw);	
				
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 				  
		return sw.toString();	   
	   }
	
	 	@Bean	
	    @ServiceActivator(inputChannel = "errorOut")
	    public MessageHandler fileWritingMessageHandler() {
	   	
	        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File("C:\\Users\\pradeep.bhati\\error"));
	        handler.setFileExistsMode(FileExistsMode.REPLACE);  
	        handler.setExpectReply(false);    
	        handler.setFileNameGenerator(a -> a.getHeaders().getId().toString());
	        handler.setAutoCreateDirectory(true); 
	        return handler;
	    }
	   	
	 	
	    @Transformer(inputChannel = "errorChannel",outputChannel=OutputProcessor.OUTPUT)
		   String transformError(Message<MessageHandlingException> message)  {
	    	System.out.println("Check inside");
	 		String msg=message.getPayload().getCause().getMessage();
	 		errorparkchannel.send(MessageBuilder.withPayload(msg).build());
			return message.getPayload().getCause().getMessage();	             
		   }
	    
//	    @Bean 
//	    public IntegrationFlow  errorFlow () {
//	    	return IntegrationFlows.from("errorChannel").bridge().channel(OutputProcessor.OUTPUT).get();
//	    	
//	    }
//	    @Bean(name = PollerMetadata.DEFAULT_POLLER)
//	    public PollerMetadata poller() {
//	        return Pollers.fixedDelay(100).get();
//	    }


}
