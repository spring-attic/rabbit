/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.rabbit.sink;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

/**
 * Tests for RabbitSink.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public abstract class RabbitSinkTests {

	@ClassRule
	public static RabbitTestSupport rabbitAvailable = new RabbitTestSupport();

	@Autowired
	protected Sink channels;

	@Autowired
	protected RabbitSinkProperties properties;

	@Autowired
	protected RabbitTemplate rabbitTemplate;

	@Autowired
	protected RabbitAdmin rabbitAdmin;

	@Autowired(required = false)
	protected MessageConverter myConverter;

	@Autowired
	protected CachingConnectionFactory bootFactory;

	@SpringBootTest({ "rabbit.routingKey=scsapp-testq",
		"rabbit.persistentDeliveryMode=true",
		"rabbit.mappedRequestHeaders=STANDARD_REQUEST_HEADERS,bar" })
	public static class SimpleRoutingKeyAndCustomHeaderTests extends RabbitSinkTests {

		@Test
		public void test() {
			this.channels.input().send(MessageBuilder.withPayload("foo".getBytes())
										.setHeader("bar", "baz")
										.setHeader("qux", "fiz")
										.build());
			this.rabbitTemplate.setReceiveTimeout(10000);
			Message received = this.rabbitTemplate.receive("scsapp-testq");
			assertEquals("foo", new String(received.getBody()));
			assertEquals("baz", received.getMessageProperties().getHeaders().get("bar"));
			assertNull(received.getMessageProperties().getHeaders().get("qux"));
			assertEquals(MessageDeliveryMode.PERSISTENT, received.getMessageProperties().getReceivedDeliveryMode());
		}

	}


	@SpringBootTest({ "rabbit.routingKey=scsapp-testOwn",
		"rabbit.own-connection=true" })
	public static class OwnConnectionTest extends RabbitSinkTests {

		@Test
		public void test() {
			this.rabbitAdmin.declareQueue(
					new Queue("scsapp-testOwn", false, false, true));
			this.bootFactory.resetConnection();
			this.channels.input().send(MessageBuilder.withPayload("foo".getBytes())
										.build());
			this.rabbitTemplate.setReceiveTimeout(10000);
			Message received = this.rabbitTemplate.receive("scsapp-testOwn");
			assertEquals("foo", new String(received.getBody()));
			assertThat(this.bootFactory.getCacheProperties().getProperty("localPort")).isEqualTo("0");
		}

	}

	@SpringBootApplication
	static class RabbitSinkApplication {

		@Bean
		public Queue queue() {
			return new Queue("scsapp-testq", false, false, true);
		}

		@Bean
		public DirectExchange exchange() {
			return new DirectExchange("scsapp-testex", false, true);
		}

		@Bean
		public Binding binding() {
			return BindingBuilder.bind(queue()).to(exchange()).with("scsapp-testrk");
		}

	}

}
