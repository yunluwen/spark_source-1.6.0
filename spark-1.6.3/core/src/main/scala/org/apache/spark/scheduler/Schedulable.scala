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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */

/**
  * Schedulable是一个特征类，Pool是其具体的实现
  * Schedulable有两个类，分别是Pool 和 TaskSetManager
  */
private[spark] trait Schedulable {
  var parent: Pool
  // child queues
  //调度队列
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  //调度模式
  def schedulingMode: SchedulingMode
  //调度池权重
  def weight: Int
  //计算资源中的cpu核数
  def minShare: Int
  //正在运行的task数量
  def runningTasks: Int
  //调度池的优先级
  def priority: Int
  //池的阶段id用于在调度中中断绑定
  def stageId: Int
  //调度池名字
  def name: String

  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  def checkSpeculatableTasks(): Boolean
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
