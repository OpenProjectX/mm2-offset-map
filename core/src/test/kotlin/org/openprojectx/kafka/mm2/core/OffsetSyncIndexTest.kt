package org.openprojectx.kafka.mm2.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class OffsetSyncIndexTest {
    @Test
    fun `translates from nearest lower source offset sync`() {
        val index = OffsetSyncIndex.from(
            listOf(
                OffsetSync("orders", 0, upstreamOffset = 10, targetOffset = 100),
                OffsetSync("orders", 0, upstreamOffset = 20, targetOffset = 200),
            ),
        )

        val translated = index.translate("orders", 0, sourceOffset = 23)

        assertEquals(203, translated?.targetOffset)
        assertEquals(20, translated?.syncUpstreamOffset)
        assertEquals(200, translated?.syncTargetOffset)
    }

    @Test
    fun `returns null when there is no usable sync`() {
        val index = OffsetSyncIndex.from(listOf(OffsetSync("orders", 0, upstreamOffset = 10, targetOffset = 100)))

        assertNull(index.translate("orders", 0, sourceOffset = 9))
        assertNull(index.translate("payments", 0, sourceOffset = 10))
    }
}
