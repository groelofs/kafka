/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package kafka.api

import scala.collection.JavaConverters._

import org.junit.Assert._
import org.junit.Test

import kafka.server.KafkaServer
import kafka.server.QuotaType

/**
 * Currently a simple proof of concept of a multi-cluster integration test, but ultimately intended
 * as a feasibility test for proxy-based federation:  specifically, can a vanilla client (whether
 * consumer, producer, or admin) talk to a federated set of two or more physical clusters and
 * successfully execute all parts of its API?
 */
class ProxyBasedFederationTest extends MultiClusterAbstractConsumerTest {
  override def numClusters: Int = 2  // need one ZK instance for each Kafka cluster [FIXME: can we "chroot" instead?]
  override def brokerCountPerCluster: Int = 3  // three _per Kafka cluster_, i.e., six total

  @Test
  def testBasicMultiClusterSetup(): Unit = {
    debug(s"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    debug(s"beginning testBasicMultiClusterSetup() with numClusters=${numClusters} and brokerCountPerCluster=${brokerCountPerCluster}")
    val numRecords = 1000
    debug(s"creating producer (IMPLICITLY FOR CLUSTER 0)")
    val producer = createProducer()
    debug(s"using producer to send $numRecords records to topic-partition $tp1 (IMPLICITLY FOR CLUSTER 0)")
    sendRecords(producer, numRecords, tp1)

    debug(s"creating consumer (IMPLICITLY FOR CLUSTER 0)")
    val consumer = createConsumer()
    debug(s"'assigning' consumer to topic-partition $tp1 ... or vice-versa (IMPLICITLY FOR CLUSTER 0)")
    consumer.assign(List(tp1).asJava)
    debug(s"seeking to beginning of topic-partition $tp1 (IMPLICITLY FOR CLUSTER 0)")
    consumer.seek(tp1, 0)
    debug(s"calling consumeAndVerifyRecords()")
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0)

    debug(s"done with functional parts; now running assertions")

    // this stuff is just ripped off from PlaintextConsumerTest testQuotaMetricsNotCreatedIfNoQuotasConfigured()
    def assertNoMetric(broker: KafkaServer, name: String, quotaType: QuotaType, clientId: String): Unit = {
        val metricName = broker.metrics.metricName(name,
                                  quotaType.toString,
                                  "",
                                  "user", "",
                                  "client-id", clientId)
        assertNull("Metric should not have been created " + metricName, broker.metrics.metric(metricName))
    }
    serversByCluster(0).foreach(assertNoMetric(_, "byte-rate", QuotaType.Produce, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Produce, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "byte-rate", QuotaType.Fetch, consumerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Fetch, consumerClientId))

    serversByCluster(0).foreach(assertNoMetric(_, "request-time", QuotaType.Request, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "request-time", QuotaType.Request, consumerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, consumerClientId))

    def assertNoExemptRequestMetric(broker: KafkaServer): Unit = {
        val metricName = broker.metrics.metricName("exempt-request-time", QuotaType.Request.toString, "")
        assertNull("Metric should not have been created " + metricName, broker.metrics.metric(metricName))
    }
    serversByCluster(0).foreach(assertNoExemptRequestMetric(_))
    debug(s"done with testBasicMultiClusterSetup()\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }

}
