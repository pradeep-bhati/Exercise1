package com.iqvia.example;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.MessageChannel;

public interface OutputProcessor extends Source{
	
	@Output("errorparkchannel")
    MessageChannel parkingLot();

}
