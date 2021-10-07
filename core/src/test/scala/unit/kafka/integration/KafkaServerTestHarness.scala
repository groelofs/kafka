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
import kafka.zk.ZooKeeperTestHarness
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
abstract class KafkaServerTestHarness extends ZooKeeperTestHarness {
  private val instanceConfigs: Buffer[Seq[KafkaConfig]] = new ArrayBuffer(numClusters)
  private val instanceServers: Buffer[Buffer[KafkaServer]] = new ArrayBuffer(numClusters)
  private val brokerLists: Buffer[String] = new ArrayBuffer(numClusters)
  private var alive: Array[Boolean] = null  // not yet multi-cluster-aware (used by only 8 tests)

  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE

  // backward-compatible accessors for existing classes that refer to one or more of these as variables (or
  // at least no-args methods):

  def servers: Buffer[KafkaServer] = serversByCluster(0)
  def brokerList: String = brokerList(0)
  def brokerList_=(listOfBrokers: String) { brokerLists(0) = listOfBrokers }
  // configs() is not currently overridden anywhere (=> made final to prevent that), but it is used in ~ five
  // other test classes, so we can't make it private
  final def configs: Seq[KafkaConfig] = configsByCluster(0)

  // extended accessors to support multi-cluster tests:

  // This one can't be named servers() (and/or can't index a Buffer[Buffer[]]) since that conflicts with
  // indexed addressing of Buffer[KafkaServer], of which there are ~90 existing instances throughout
  // core/src/test. (Well, it _could_ if Scala required parens for no-args method calls, but it doesn't,
  // so here we are.)  However, clusterId-indexing is new, and the sole users of the extended syntax are
  // ProxyBasedFederationTest and its superclasses (including us), so we can make do with asymmetric naming
  // of this extended getter:
  def serversByCluster(clusterId: Int): Buffer[KafkaServer] = {
    instanceServers(clusterId)
  }

  def brokerList(clusterId: Int): String = {
    brokerLists(clusterId)
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
    if (instanceConfigs(clusterId) == null) {
      if (clusterId == 0)
        instanceConfigs(clusterId) = generateConfigs
      else
        instanceConfigs(clusterId) = generateConfigsByCluster(clusterId)
    }
    instanceConfigs(clusterId)
  }

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigs: Seq[KafkaConfig]

  /**
   * Multi-cluster implementations must override this method as well, but single-cluster ones (the vast majority)
   * can ignore it.
   */
  def generateConfigsByCluster(clusterId: Int): Seq[KafkaConfig] = {
    if (clusterId > 0)
      throw new KafkaException("non-zero clusterId (" + clusterId + ") is not supported: must override this method!")
    generateConfigs
  }

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If the ACLs
   * are only set later, tests may fail. The failure could manifest itself as a cluster action
   * authorization exception when processing an update metadata request (controller -> broker) or in more obscure
   * ways (e.g. __consumer_offsets topic replication fails because the metadata cache has no brokers as a previous
   * update metadata request failed due to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart(): Unit = {}

  /**
   * Override this in case Tokens or security credentials needs to be created after `servers` are started.
   * The default implementation of this method is a no-op.
   */
  def configureSecurityAfterServersStart(): Unit = {}

  // single-cluster only:
  def serverForId(id: Int): Option[KafkaServer] = servers.find(s => s.config.brokerId == id)

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

    if (configs.isEmpty)  // intentionally not extending this questionable(?) check to clusterId > 0
      throw new KafkaException("Must supply at least one server config.")

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityBeforeServersStart()

    // if this ever changes, both method-calls should be moved _inside_ the numClusters loop (and clusterId
    // passed as an argument to both):
    if (numClusters > 1)
      // the do-nothing versions in this class are fine, but many overrides use the no-args (clusterId == 0)
      // accessors for zkClient and zkConnect:
      warn(s"setting up $numClusters Kafka clusters, but configureSecurityBeforeServersStart() and configureSecurityAfterServersStart() are NOT set up for multi-cluster support!")

    (0 until numClusters).map { clusterId =>

      instanceServers += new ArrayBuffer(configsByCluster(clusterId).length)

      // Add each broker to `instanceServers` buffer as soon as it is created to ensure that brokers
      // are shut down cleanly in tearDown even if a subsequent broker fails to start
      for (config <- configsByCluster(clusterId))
        instanceServers(clusterId) += TestUtils.createServer(config, time = brokerTime(config.brokerId))
      brokerLists += TestUtils.bootstrapServers(instanceServers(clusterId), listenerName)
      alive = new Array[Boolean](instanceServers(clusterId).length)
      Arrays.fill(alive, true)

    }

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityAfterServersStart()
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

  /**
   * Picks a broker at random and kills it if it isn't already dead.  (Multi-cluster not supported; cluster ID is
   * always zero.)  Returns the id of the broker killed.
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(servers.length)
    killBroker(index)
    index
  }

  // multi-cluster not supported: clusterId is always zero (i.e., uses alive() and servers(), not serversByCluster(Int))
  def killBroker(index: Int): Unit = {
    if(alive(index)) {
      servers(index).shutdown()
      servers(index).awaitShutdown()
      alive(index) = false
    }
  }

  /**
   * Restart any dead brokers
   */
  // multi-cluster not supported: clusterId is always zero (i.e., uses alive() and servers(), not serversByCluster(Int))
  def restartDeadBrokers(): Unit = {
    for(i <- servers.indices if !alive(i)) {
      servers(i).startup()
      alive(i) = true
    }
  }
}
