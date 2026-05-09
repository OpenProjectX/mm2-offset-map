package org.openprojectx.kafka.mm2.core

import java.nio.ByteBuffer
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Type

object OffsetSyncDecoder {
    private val keySchema = Schema(
        Field("topic", Type.STRING),
        Field("partition", Type.INT32),
    )

    private val valueSchema = Schema(
        Field("upstreamOffset", Type.INT64),
        Field("offset", Type.INT64),
    )

    fun decode(key: ByteArray, value: ByteArray): OffsetSync {
        val decodedKey = keySchema.read(ByteBuffer.wrap(key))
        val decodedValue = valueSchema.read(ByteBuffer.wrap(value))

        return OffsetSync(
            topic = decodedKey.getString("topic"),
            partition = decodedKey.getInt("partition"),
            upstreamOffset = decodedValue.getLong("upstreamOffset"),
            targetOffset = decodedValue.getLong("offset"),
        )
    }
}
