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

import java.security.MessageDigest
import java.util.UUID

object GuidV5 {
    def nameUUIDFrom(name: String): UUID = {
        val sha1 = MessageDigest.getInstance("SHA-1")
        sha1.update(name.getBytes("UTF-8"))

        val data = sha1.digest().take(16)
        data(6) = (data(6) & 0x0f).toByte
        data(6) = (data(6) | 0x50).toByte // set version 5
        data(8) = (data(8) & 0x3f).toByte
        data(8) = (data(8) | 0x80).toByte

        var msb = 0L
        var lsb = 0L

        for (i <- 0 to 7)
            msb = (msb << 8) | (data(i) & 0xff)

        for (i <- 8 to 15)
            lsb = (lsb << 8) | (data(i) & 0xff)

        val mostSigBits = msb
        val leastSigBits = lsb

        new UUID(mostSigBits, leastSigBits)
    }

    def isValidUUID(uuid: String): Boolean = {
        // Is the token a UUID?
        try{
            UUID.fromString(uuid)
            true
        } catch {
            case e: IllegalArgumentException =>
                false
        }
    }
}