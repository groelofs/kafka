/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.integration

import java.io.File
import java.util.Arrays

import kafka.server._
import kafka.utils.TestUtils
import kafka.zk.MultiClusterZooKeeperTestHarness
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.junit.{After, Before}

import scala.collection.Seq
import scala.collection.mutable.{ArrayBuffer, Buffer}
import java.util.Properties

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Time

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class MultiClusterKafkaServerTestHarness extends MultiClusterZooKeeperTestHarness {
  private val brokerLists: Buffer[String] = new ArrayBuffer(numClusters)
  private val instanceServers: Buffer[Buffer[KafkaServer]] = new ArrayBuffer(numClusters)
  private val instanceConfigs: Buffer[Seq[KafkaConfig]] = new ArrayBuffer(numClusters)

  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE

  def brokerList(clusterId: Int): String = {
    brokerLists(clusterId)
  }

  // We avoid naming this method servers() since that conflicts with the existing indexed addressing of
  // Buffer[KafkaServer] (i.e., of specific brokers within a single cluster) used in our sub-sub-subclass
  // AbstractConsumerTest.
  def serversByCluster(clusterId: Int): Buffer[KafkaServer] = {
    instanceServers(clusterId)
  }

  // Ditto on the naming.  In addition, the key helper method generateConfigs() is overridden by more than
  // two dozen other test classes.  Ergo, we can't unconditionally switch to generateConfigsByCluster(Int)
  // since that would break compatibility for those tests by bypassing their overrides of the clusterId == 0
  // version.  But conditionally calling either generateConfigs() or generateConfigsByCluster(Int) is fully
  // backward-compatible (even if it's a bit ugly):
  private def configsByCluster(clusterId: Int): Seq[KafkaConfig] = {
    if (instanceConfigs.length < numClusters)
      (instanceConfigs.length until numClusters).foreach { _ => instanceConfigs += null }
    if (clusterId >= numClusters)
      throw new KafkaException("clusterId (" + clusterId + ") is out of range: must be less than " + numClusters)
    if (instanceConfigs(clusterId) == null)
      instanceConfigs(clusterId) = generateConfigsByCluster(clusterId)
    instanceConfigs(clusterId)
  }

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigsByCluster(clusterId: Int): Seq[KafkaConfig]

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If
   * the ACLs are only set later, tests may fail. The failure could manifest itself as a cluster action
   * authorization exception when processing an update metadata request (controller -> broker) or in more obscure
   * ways (e.g. __consumer_offsets topic replication fails because the metadata cache has no brokers as a previous
   * update metadata request failed due to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart(clusterId: Int): Unit = {}

  /**
   * Override this in case Tokens or security credentials needs to be created after `servers` are started.
   * The default implementation of this method is a no-op.
   */
  def configureSecurityAfterServersStart(clusterId: Int): Unit = {}

//def configs: Seq[KafkaConfig] = {
//  if (instanceConfigs == null)
//    instanceConfigs = generateConfigs
//  instanceConfigs
//}

  // TODO:  extend for multi-cluster or omit?
//def serverForId(id: Int): Option[KafkaServer] = servers.find(s => s.config.brokerId == id)

  def boundPort(server: KafkaServer): Int = server.boundPort(listenerName)

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None
  protected def brokerTime(brokerId: Int): Time = Time.SYSTEM

  @Before
  override def setUp(): Unit = {
    super.setUp()

    (0 until numClusters).map { clusterId =>
      // apparently intended semantic is that each config in the Seq[KafkaConfig] corresponds to a single broker?
      if (configsByCluster(clusterId).isEmpty)
        throw new KafkaException("Must supply at least one server config for cluster #${clusterId}")

      instanceServers += new ArrayBuffer(configsByCluster(clusterId).length)

      // default implementation is a no-op, it is overridden by subclasses if required
      configureSecurityBeforeServersStart(clusterId)

      // Add each broker to `instanceServers` buffer as soon as it is created to ensure that brokers
      // are shut down cleanly in tearDown even if a subsequent broker fails to start
      for (config <- configsByCluster(clusterId))
        instanceServers(clusterId) += TestUtils.createServer(config, time = brokerTime(config.brokerId))
      brokerLists += TestUtils.bootstrapServers(instanceServers(clusterId), listenerName)

      // default implementation is a no-op, it is overridden by subclasses if required
      configureSecurityAfterServersStart(clusterId)
    }
  }

  @After
  override def tearDown(): Unit = {
    (0 until numClusters).map { clusterId =>
      if (serversByCluster(clusterId) != null) {
        TestUtils.shutdownServers(serversByCluster(clusterId))
      }
    }
    super.tearDown()
  }

  /**
   * Create a topic in ZooKeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1,
                  topicConfig: Properties = new Properties, clusterId: Int = 0):
                  scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient(clusterId), topic, numPartitions, replicationFactor, serversByCluster(clusterId),
      topicConfig)

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]], clusterId: Int):
                  scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient(clusterId), topic, partitionReplicaAssignment, serversByCluster(clusterId))

}
