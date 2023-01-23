package net.idata.pipeline.util

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

object ElapsedTimeUtil {
    def getElapsedTime(durationMillis: Long): (String, Boolean) = {
        var seconds: Long = durationMillis / 1000
        var minutes: Long = {
            if (seconds > 0) {
                val min = seconds / 60
                seconds = seconds - (min * 60)
                min
            } else
                0
        }
        val hours: Long = {
            if (minutes > 0) {
                val hr = minutes / 60
                minutes = minutes - (hr * 60)
                hr
            } else
                0
        }
        if (hours > 0) {
            if (hours > 8)
                ("timed out", true)
            else
                (hours.toString + " hr " + minutes.toString + " min " + seconds + " sec", false)
        }
        else if (minutes > 0)
            (minutes.toString + " min " + seconds + " sec", false)
        else
            (seconds + " sec", false)
    }
}
