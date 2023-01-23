package net.idata.pipeline.model

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import scala.collection.mutable.ListBuffer

object GlobalJobContext {
    //private object Locker
    private val jobContexts: ListBuffer[JobContext] = new ListBuffer()

    def getAll: ListBuffer[JobContext] = jobContexts

    //def getNumberOfRunningJobs: Int = jobContexts.count(_.state == RUNNING)

    def addJobContext(jobContext: JobContext): Unit = {
        synchronized {
            jobContexts += jobContext
        }
    }

    def deleteJobContext(jobContext: JobContext): Unit = {
        synchronized {
            val job = jobContexts.find(_.pipelineToken.compareTo(jobContext.pipelineToken) == 0)
                .getOrElse(throw new PipelineException("Internal error - could not find the JobContext for pipeline token: " + jobContext.pipelineToken))
            jobContexts -= job
        }
    }

    def replaceJobContext(jobContext: JobContext): Unit = {
        synchronized {
            deleteJobContext(jobContext)
            addJobContext(jobContext)
        }
    }

    def findJobContext(pipelineToken: String): JobContext = {
        jobContexts.find(jobContext => jobContext.pipelineToken.compareTo(pipelineToken) == 0).orNull
    }
}
