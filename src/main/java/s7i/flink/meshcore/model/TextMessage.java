package s7i.flink.meshcore.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TextMessage implements Serializable {

    private String channelHash;
    private Long timestamp;
    private String from;
    private String message;
}
