package net.idata.pipeline.util

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
