package org.openprojectx.kafka.mm2.core

import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals

class OffsetSyncDecoderTest {
    @Test
    fun `decodes mirror maker offset sync record`() {
        val key = Base64.getDecoder().decode("AAl0b3BpY25hbWUAAAAA")
        val value = Base64.getDecoder().decode("AAAAAAACXsoAAAAAAAHMfw==")

        val sync = OffsetSyncDecoder.decode(key, value)

        assertEquals("topicname", sync.topic)
        assertEquals(0, sync.partition)
        assertEquals(155338, sync.upstreamOffset)
        assertEquals(117887, sync.targetOffset)
    }
}
