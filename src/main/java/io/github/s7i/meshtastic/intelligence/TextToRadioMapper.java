package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.Constants;
import com.geeksville.mesh.MeshProtos.Data;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import com.geeksville.mesh.MeshProtos.ToRadio;
import com.geeksville.mesh.Portnums.PortNum;
import com.google.protobuf.ByteString;
import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import java.time.Instant;

public interface TextToRadioMapper {


    PacketIdGenerator packetIdGenerator();

    default byte[] toToRadioData(SendMessage sendMessage) {
        var payload = ByteString.copyFromUtf8(sendMessage.getMesssage());

        if (payload.size() > Constants.DATA_PAYLOAD_LEN_VALUE) {
            throw new IllegalStateException("payload too big");
        }

        var rx = ToRadio.newBuilder()
              .setPacket(MeshPacket.newBuilder()
                    .setId(packetIdGenerator().generatePacketId())
//                    .setFrom(fromNode)
                    .setWantAck(true)
                    .setTo(sendMessage.getToNode())
                    .setRxTime((int) Instant.now().getEpochSecond())
                    .setChannel(sendMessage.getChannel())
                    .setDecoded(Data.newBuilder()
                          .setPayload(payload)
                          .setPortnum(PortNum.TEXT_MESSAGE_APP)))
              .build();
        return rx.toByteArray();
    }

}
