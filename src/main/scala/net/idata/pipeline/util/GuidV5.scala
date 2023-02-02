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

Author(s): Todd Fearn
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