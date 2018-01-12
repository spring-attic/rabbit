/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.rabbit.sink;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.Expression;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.AmqpOutboundEndpointSpec;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;

/**
 * A sink module that sends data to RabbitMQ.
 *
 * @author Gary Russell
 * @author Chris Schaefer
 */
@EnableBinding(Sink.class)
@EnableConfigurationProperties(RabbitSinkProperties.class)
public class RabbitSinkConfiguration {

	@Autowired
	private RabbitSinkProperties properties;

	@Value("#{${rabbit.converterBeanName:null}}")
	private MessageConverter messageConverter;

	@ServiceActivator(inputChannel = Sink.INPUT)
	@Bean
	public MessageHandler amqpChannelAdapter(ConnectionFactory rabbitConnectionFactory) {
		AmqpOutboundEndpointSpec handler = Amqp.outboundAdapter(rabbitTemplate(rabbitConnectionFactory))
				.mappedRequestHeaders(properties.getMappedRequestHeaders())
				.defaultDeliveryMode(properties.getPersistentDeliveryMode() ? MessageDeliveryMode.PERSISTENT
						: MessageDeliveryMode.NON_PERSISTENT);

		Expression exchangeExpression = this.properties.getExchangeExpression();
		if (exchangeExpression != null) {
			handler.exchangeNameExpression(exchangeExpression);
		}
		else {
			handler.exchangeName(this.properties.getExchange());
		}

		Expression routingKeyExpression = this.properties.getRoutingKeyExpression();
		if (routingKeyExpression != null) {
			handler.routingKeyExpression(routingKeyExpression);
		}
		else {
			handler.routingKey(this.properties.getRoutingKey());
		}

		return handler.get();
	}

	@Bean
	public RabbitTemplate rabbitTemplate(ConnectionFactory rabbitConnectionFactory) {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
		if (this.messageConverter != null) {
			rabbitTemplate.setMessageConverter(this.messageConverter);
		}
		return rabbitTemplate;
	}

	@Bean
	@ConditionalOnProperty(name = "rabbit.converterBeanName", havingValue = RabbitSinkProperties.JSON_CONVERTER)
	public Jackson2JsonMessageConverter jsonConverter() {
		return new Jackson2JsonMessageConverter();
	}

}
