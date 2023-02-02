package net.idata.pipeline.model

/*
IData Pipeline
Copyright (C) 2023 IData Corporation (http://www.idata.net)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Author(s): Todd Fearn
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
