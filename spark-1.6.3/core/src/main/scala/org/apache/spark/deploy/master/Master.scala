/*
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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Random

import org.apache.hadoop.fs.Path

import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ThreadUtils, SignalLogger, Utils}

/**
  * spark启动过程中主要是进行master 和 worker之间的通信，首先由worker节点向master发送注册信息，
  * 然后master处理完毕后，返回注册成功消息或失败消息，如果注册成功，则worker定时发送心跳消息给
  * master。
  *
  * 用户提交应用程序时，应用程序的sparkContext会向master发送应用注册消息，并由master给该应用分配executor,
  * executor启动后，executor会向sparkContext发送注册成功消息，sparkContext的RDD触发行动操作后，将创建
  * RDD的DAG,通过DAGScheduler进行划分stage,并将stage转化为taskSet,接着由taskScheduler向注册的executor
  * 发送执行消息，executor接收到任务消息后启动并运行，最后当所有任务运行时，由Driver处理结果并回收资源。
  *
  * master分析重点：
  * master主备切换机制，master注册机制(worker的注册,driver的注册，applciation的注册)
  * master状态转换机制，master资源调度机制
  *
  * master 是一个线程
  * @param rpcEnv
  * @param address
  * @param webUiPort
  * @param securityMgr
  * @param conf
  */
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val rebuildUIThread =
    ThreadUtils.newDaemonSingleThreadExecutor("master-rebuild-ui-thread")
  private val rebuildUIContext = ExecutionContext.fromExecutor(rebuildUIThread)

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss") // For application IDs

  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
  // Using ConcurrentHashMap so that master-rebuild-ui-thread can add a UI after asyncRebuildUI
  private val appIdToUI = new ConcurrentHashMap[String, SparkUI]

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host, "Expected hostname")

  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  //可以手动设置的master资源调度算法
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    rebuildUIThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    /**
      * master主备切换机制入口
      */
    case ElectedLeader => {
      //从持久化引擎中获取数据，driver,worker,app 等的信息
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        //如果APP、driver、worker是空的，recoverystate设置为alive
        RecoveryState.ALIVE
      } else {
        //有一个不为空则设置为recovering(恢复中。。。)
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        //判断状态如果为recovering 恢复中，将storedApps，storedDrier，storeWorkers重新注册到master内部缓存结构中
        //开始恢复
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            //调用自己的CompleteRecovery()方法
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }

    /**
      * spark运行程序过程中第一步：1.2
      * 接收到ClientEndPoint注册应用消息后，作出响应。
      * 处理Application注册的请求
      */
    case RegisterApplication(description, driver) => {
      // TODO Prevent repeated registrations from some driver
      //判断master的状态，如果是STANDBY(不是active)，就不做处理
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        //用ApplicationDescription创建ApplicationInfo
        val app = createApplication(description, driver)
        //记录应用信息，并把该应用加入到等待运行应用列表中。
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        //使用持久化引性，将applicationInfo进行持久化
        persistenceEngine.addApplication(app)
        //注册完毕后，发送成功消息RegisteredApplication给SparkDeploySchedulerBackend的ClientEndPoint
        driver.send(RegisteredApplication(app.id, self))

        //在scheduler()方法中调用startExecutorOnWorkers方法运行应用，在执行前需要获取运行应用的worker，然后发送
        //LaunchExecutor消息给Worker,通知worker启动Executor.
        //master接收到Application的注册请求之后，会使用自己的资源调度算法，在spark集群的worker上，为这个application
        //启动多个Executor.
        schedule()
      }
    }

    /**
      * master 状态改变机制，Executor
      */
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      //找到executor对应的app,然后在反过来通过app内部的executor缓存的executor信息
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        //有值
        case Some(exec) => {
          //设置app的当前状态
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state
          //如果executor的状态是正在运行的
          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }
          //向driver同步发送ExecutorUpdated消息
          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus))

          //判断，如果executor完成了
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              //从app的缓存中移除executor
              appInfo.removeExecutor(exec)
            }
            //从运行executor的缓存中移除executor
            exec.worker.removeExecutor(exec)
            //判断，如果executor的退出状态是非正常的
            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            //判断application当前的重试次数，是否达到了最大值10
            if (!normalExit
                && appInfo.incrementRetryCount() >= MAX_EXECUTOR_RETRIES
                && MAX_EXECUTOR_RETRIES >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values

              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                //executor反复调度都是失败
                //移除application
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        }
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    }

    /**
      * master 状态改变机制，Driver
      */
    case DriverStateChanged(driverId, state, exception) => {
      state match {
        //如果driver的状态是出错 或者 完成 ，被杀掉，失败，就移除driver
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId, worker) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }

    case AttachCompletedRebuildUI(appId) =>
      // An asyncRebuildSparkUI has completed, so need to attach to master webUi
      Option(appIdToUI.get(appId)).foreach { ui => webUi.attachSparkUI(ui) }
  }

  /**
    * spark启动过程中的通信：第一步 1.2
    * 在master中，master接收到worker的注册消息后，先判断master当前状态是否处于STANDBY状态，
    * 如果是则忽略该消息，如果在注册列表中发现了该worker的编号，则发送注册失败的消息，判断完毕后，
    * 使用registerWorker方法把该worker加入到worker列表中，用于集群处理任务时进行调度。
    *
    * @param context
    * @return
    */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(
        id, workerHost, workerPort, workerRef, cores, memory, workerUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      //master处于STANDBY状态，返回"master"处于"STANDBY"的消息
      if (state == RecoveryState.STANDBY) {
        context.reply(MasterInStandby)
        //注册列表中发现了该worker的编号，则发送注册失败的消息
      } else if (idToWorker.contains(id)) {
        context.reply(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerUiPort, publicAddress)
        //registerWorker方法中注册worker,该方法会把worker放到列表中，用于后续运行任务时使用
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          context.reply(RegisteredWorker(self, masterWebUiUrl))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          context.reply(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
    }

    case RequestSubmitDriver(description) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }
    }

    case RequestKillDriver(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send((driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }
    }

    case RequestDriverStatus(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }
    }

    case RequestMasterState => {
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))
    }

    case BoundPortsRequest => {
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))
    }

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker)
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  /**
    * master主备切换机制，开始恢复
    * @param storedApps
    * @param storedDrivers
    * @param storedWorkers
    */
  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        //重新注册application
        registerApplication(app)
        //将application状态设置为unknown
        app.state = ApplicationState.UNKNOWN
        //向work中的driver发送masterChanged消息
        //向Application所对应的driver,以及Worker发送Standby Master的地址
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }
    //将storedDrivers重新加入内存缓存中
    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }
    //将storedWorkers重新加入内存缓存中
    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        //重新注册worker
        registerWorker(worker)
        //将worker状态修改为unknown
        worker.state = WorkerState.UNKNOWN
        //向work发送masterChanged
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  /**
    * 完成master的主备切换，从字面意思上理解，就是完成master的恢复
    * 基于文件系统的主备切换，需要手动重启master
    * 基于zookeeper的主备切换，可以自动重启master
    *
    * 通过beginRecovery方法之后，对没有发送响应消息的Driver和Worker进行处理，
    * 过滤掉他们的信息。
    */
  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    if (state != RecoveryState.RECOVERING) { return }
    //将状态修改为正在恢复
    state = RecoveryState.COMPLETING_RECOVERY
    //使用短的同步时间确保“只有一次”恢复语义。

    //将Application和worker,过滤出来目前状态还是unknow的
    //然后遍历，分别调用removeWorker和finishApplication方法，对可能已经出故障或者
    //甚至已经死掉的application和worker进行清理。
    //总结一下清理的机制，三点：1.从内存缓存结构中移除  2.从相关的组件的内存缓存中移除
    //3.从持久化存储中移除
    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    //重新调度 那些没有回应worker的 drivers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        //重启Driver
        relaunchDriver(d)
      } else {
        //删除driver
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }
    //将state转为alive，代表恢复完成
    state = RecoveryState.ALIVE
    //重新调用schedule（）恢复完成
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   *
   * Application的调度机制(重点！！！)
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    // 用来记录每个worker已经分配的核数
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    // 用来记录每个worker已经分配的executor数
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    // 剩余总共资源
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    //判断是否能启动Executor
    def canLaunchExecutor(pos: Int): Boolean = {
      // 条件1 ：若集群剩余core >= spark.executor.cores
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      // 条件2： 若该Worker上的剩余core >= spark.executor.cores
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      // 条件3： 若设置了spark.executor.cores
      // 或者 该Worker还未分配executor
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        // 条件4：若该Worker上的剩余内存 >= spark.executor.memory
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        // 条件5： 若分配了该executor后，
        // 总共分配的core数量 <= spark.cores.max
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        //若满足 条件3，
        //且满足 条件1，条件2，条件4，条件5
        //则返回True
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        //若不满足 条件3，
        //即一个worker只有一个executor
        //且满足 条件1，条件2
        //也返回True。
        // 返回后，不会增加 assignedExecutors
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    //过滤去能启动executor的Worker
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    //调度资源，
    //直到worker上的executor被分配完
    //注意：
    //一个app会尽可能的使用掉集群的所有资源，所以设置spark.cores.max参数是非常有必要的！
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          // minCoresPerExecutor 是用户设置的 spark.executor.cores
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 若用户没有设置 spark.executor.cores
          // 则oneExecutorPerWorker就为True
          // 也就是说，assignedCores中的core都被一个executor使用
          // 若用户设置了spark.executor.cores，
          // 则该Worker的assignedExecutors会加1
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          //资源分配算法有两种：
          // 1. 尽量打散，将一个app尽可能的分配到不同的节点上，
          // 这有利于充分利用集群的资源，
          // 在这种情况下，spreadOutApps设为True，
          // 于是，该worker分配好了一个executor之后就退出循环
          // 轮询到下一个worker
          // 2. 尽量集中，将一个app尽可能的分配到同一个的节点上，
          // 这适合cpu密集型而内存占用比较少的app
          // 在这种情况下，spreadOutApps设为False，
          // 于是，继续下一轮的循环
          // 在该Worker上分配executor
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
   * 调度 启动 worker上的 executors
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    //使用FIFO调度算法运行应用，即先注册的应用先运行。
    //注意这里
    //若在该次资源调度中该app并没有启动足够的executor，等到集群资源变化时，会再次资源调度，
    //在waitingApp中遍历到该app，其coresLeft > 0。
    for (app <- waitingApps if app.coresLeft > 0) {
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor

      // Filter out workers that don't have enough resources to launch an executor
      //找出符合剩余内存大于等于启动Executor所需大小，核数大于等于1条件的worker
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse

      //确定运行在哪些worker上和每个worker分配用于运行的核数，分配算法由两种：一种是把应用运行在尽可能多的worker上，
      //相反，另一种是运行在尽可能少的worker上
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      //通知分配的worker启动worker
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   *
   * 实现在worker中启动Executor
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    //注意：
    // 该work上的executor数量
    // 若没设置 spark.executor.cores
    // 则为1
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    //注意：
    // 分配给一个executor的core数量
    // 若没设置 spark.executor.cores
    // 则为该worker上所分配的所有core的数量
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      //创建该executor信息
      //并把它加入到app信息中
      //并返回executor信息
      val exec = app.addExecutor(worker, coresToAssign)
      //启动Executor
      launchExecutor(worker, exec)
      //将当前Application的状态设置为RUNNING
      /**
        * 这句代码并不是将该app从waitingApp队列中去除。若在该次资源调度中该app
        * 并没有启动足够的executor，等到集群资源变化时，会再次资源调度，在waitingApp
        * 中遍历到该app，其coresLeft > 0。
        */
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   * master 资源调度
   */
  private def schedule(): Unit = {
    //首先判断master状态不是alive的，直接返回
    //也就是说，standBy master是不会进行application等资源调度的
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    //获取运行的worker列表,并将传入的集合中元素顺序随机打乱。
    //取出了workers中所有之前注册上来的worker,进行过滤，必须状态是alive的worker
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    //首先，调度driver
    //只有在yarn-cluster的时候才会注册driver,因为standalone和yarn-client都是在本地启动driver
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      //遍历活着的worker,且当前driver未启动，也就是launch为false
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        //如果当前worker的空闲内存量大于等于driver需要的内存
        //并且worker的空闲cpu数量，大于等于driver需要的cpu数量
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          //启动driver
          launchDriver(worker, driver)
          //并且将driver从waitingDrivers等待队列中移除
          waitingDrivers -= driver
          launched = true
        }
        //将指针指向下一个worker
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    //启动worker上的executor
    startExecutorsOnWorkers()
  }

  /**
    * spark程序运行过程中通信:  第二步  2.1
    * 向worker发送LaunchExecutor(启动Executor)的消息
    * @param worker
    * @param exec
    */
  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    //获取worker的终端点引用
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    //向worker发送launchExecutor的信息
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  /**
    * 重新注册worker
    * @param worker
    * @return
    */
  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.

    //在同一个节点上可能有一个或多个死掉的worker（不同ID），删除它们。
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        //从UNKNOWN注册的worker意味着worker在恢复期间重新启动。
        //因此，老worker必须死亡，所以我们会把它删除并接受新的worker。
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }
    //保存workerInfo到wokers（hashmap）中
    workers += worker
    //保存worker的id到idToWorker（hashmap）中
    idToWorker(worker.id) = worker
    //将work端点的地址保存起来
    addressToWorker(workerAddress) = worker
    true
  }

  /**
    * 删除旧的worker
    * @param worker
    */
  private def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    //将work状态修改为dead
    worker.setState(WorkerState.DEAD)
    //从idToWorker(hashmap)中去掉workid
    idToWorker -= worker.id
    //从addressToWorker(hashmap)中去掉worker.endpoint.address
    addressToWorker -= worker.endpoint.address
    //遍历worker上的executor
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      //向driver中发送executor状态改变
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None))
      //从application中删除掉这些executor
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        //重新启动
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        //删除driver
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    //持久化引擎删除worker
    persistenceEngine.removeWorker(worker)
  }

  /**
    * 重启Driver
    * @param driver
    */
  private def relaunchDriver(driver: DriverInfo) {
    //将driver的worker设置为None
    driver.worker = None
    //将driver的状态设置为relaunching（重新调度(启动)）
    driver.state = DriverState.RELAUNCHING
    //将当前的driver重新加入waitingDrivers队列
    waitingDrivers += driver
    //重新开始任务调度
    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  /**
    * 注册application
    * @param app
    */
  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }
    //spark测量系统通注册appsource
    applicationMetricsSystem.registerSource(app.appSource)
    //将APP加入内存缓存中
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    //将app加入等待调度的队列，FIFO的算法
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  /**
    * 删除application
    * @param app
    * @param state
    */
  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      //从application队列(hashset)中删除当前application
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach( a => {
          Option(appIdToUI.remove(a.id)).foreach { ui => webUi.detachSparkUI(ui) }
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      //加入已完成的application队列
      completedApps += app // Remember it in our history
      //从当前等待运行的application队列中删除当前APP
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      asyncRebuildSparkUI(app)
      //遍历当前application运行的所有executor
      for (exec <- app.executors.values) {
        //停止executor
        killExecutor(exec)
      }
      app.markFinished(state)
      //如果application是未完成的状态
      if (state != ApplicationState.FINISHED) {
        //从driver中删除application
        app.driver.send(ApplicationRemoved(state.toString))
      }
      //从持久化引擎中删除application
      persistenceEngine.removeApplication(app)
      //从新调度任务
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      //告诉说有的worker，APP已经启动完成了，所以他们可以清空APP state
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /**
   * Rebuild a new SparkUI from the given application's event logs.
   * Return the UI if successful, else None
   */
  private[master] def rebuildSparkUI(app: ApplicationInfo): Option[SparkUI] = {
    val futureUI = asyncRebuildSparkUI(app)
    Await.result(futureUI, Duration.Inf)
  }

  /** Rebuild a new SparkUI asynchronously to not block RPC event loop */
  private[master] def asyncRebuildSparkUI(app: ApplicationInfo): Future[Option[SparkUI]] = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
    val eventLogDir = app.desc.eventLogDir
      .getOrElse {
        // Event logging is disabled for this application
        app.appUIUrlAtHistoryServer = Some(notFoundBasePath)
        return Future.successful(None)
      }
    val futureUI = Future {
      val eventLogFilePrefix = EventLoggingListener.getLogPath(
        eventLogDir, app.id, appAttemptId = None, compressionCodecName = app.desc.eventLogCodec)
      val fs = Utils.getHadoopFileSystem(eventLogDir, hadoopConf)
      val inProgressExists = fs.exists(new Path(eventLogFilePrefix +
        EventLoggingListener.IN_PROGRESS))

      val eventLogFile = if (inProgressExists) {
        // Event logging is enabled for this application, but the application is still in progress
        logWarning(s"Application $appName is still in progress, it may be terminated abnormally.")
        eventLogFilePrefix + EventLoggingListener.IN_PROGRESS
      } else {
        eventLogFilePrefix
      }

      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName, HistoryServer.UI_PATH_PREFIX + s"/${app.id}", app.startTime)
      try {
        replayBus.replay(logInput, eventLogFile, inProgressExists)
      } finally {
        logInput.close()
      }

      Some(ui)
    }(rebuildUIContext)

    futureUI.onSuccess { case Some(ui) =>
      appIdToUI.put(app.id, ui)
      // `self` can be null if we are already in the process of shutting down
      // This happens frequently in tests where `local-cluster` is used
      if (self != null) {
        self.send(AttachCompletedRebuildUI(app.id))
      }
      // Application UI is successfully rebuilt, so link the Master UI to it
      // NOTE - app.appUIUrlAtHistoryServer is volatile
      app.appUIUrlAtHistoryServer = Some(ui.basePath)
    }(ThreadUtils.sameThread)

    futureUI.onFailure {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir.get}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.appUIUrlAtHistoryServer = Some(notFoundBasePath + s"?msg=$msg&title=$title")

      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.appUIUrlAtHistoryServer =
            Some(notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title")
    }(ThreadUtils.sameThread)

    futureUI
  }

  /** Generate a new app ID given a app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
    * 在某一个worker上启动driver
    * @param worker
    * @param driver
    */
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    //将driver加入worker的内存缓存结构
    //将worker内使用的内存和cpu数量，都加上driver需要的内存和cpu数量
    worker.addDriver(driver)
    //同时把worker也加入到driver内部的缓存结构中
    driver.worker = Some(worker)
    //发送LaunchDriver消息，让worker来启动driver
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    //将driver的运行状态设置为RUNNING
    driver.state = DriverState.RUNNING
  }

  /**
    * 删除driver
    * @param driverId
    * @param finalState
    * @param exception
    */
  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]) {
    //用Scala高阶函数find()根据driverId，查找到driver
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        //将driver将内存缓存中删除
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        //将driver加入到已经完成的completeDrivers
        completedDrivers += driver
        //从持久化引擎中删除driver的持久化信息
        persistenceEngine.removeDriver(driver)
        //设置driver状态设置为完成
        driver.state = finalState
        driver.exception = exception
        //从worker中遍历删除传入的driver
        driver.worker.foreach(w => w.removeDriver(driver))
        //重新调用schedule
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   *   启动消息通信的代码
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
