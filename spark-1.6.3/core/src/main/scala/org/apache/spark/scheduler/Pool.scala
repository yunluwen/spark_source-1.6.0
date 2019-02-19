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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */

/**
  * 调度算法，根据调度模式初始化算法。org.apache.spark.scheduler.SchedulingAlgorithm。
  * 调度池则用于调度每个sparkContext运行时并存的多个互相独立无依赖关系的任务集。
  * 调度池负责管理下一级的调度池和TaskSetManager对象。
  * 用户可以通过配置文件定义调度池和TaskSetManager对象。
  * 　　1.调度的模式Scheduling mode：用户可以设置FIFO或者FAIR调度方式。
  * 　　2.weight，调度的权重，在获取集群资源上权重高的可以获取多个资源。
  * 　　3.miniShare：代表计算资源中的cpu核数。
  * 配置conf/faurscheduler.xml配置调度池的属性，同时要在sparkConf对象中配置属性。
  *
  * @param poolName
  * @param schedulingMode
  * @param initMinShare
  * @param initWeight
  */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable
  with Logging {

  //调度队列
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  //调度对应关系
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  //调度池权重
  var weight = initWeight
  //计算资源中的cpu核数
  var minShare = initMinShare
  //正在运行的task数量
  var runningTasks = 0
  //优先级
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  //池的阶段id用于在调度中终端绑定
  var stageId = -1
  //调度池名字
  var name = poolName
  var parent: Pool = null

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    //调度模式
    schedulingMode match {
      case SchedulingMode.FAIR =>  //FAIR(公平调度)
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>  //FIFO，默认是FIFO的方式。
        new FIFOSchedulingAlgorithm()
    }
  }

  /**
    * Pool直接管理的是TaskSetManager，每个TaskSetManager创建时都存储了其对应的StageID.
    * @param schedulable
    */
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
