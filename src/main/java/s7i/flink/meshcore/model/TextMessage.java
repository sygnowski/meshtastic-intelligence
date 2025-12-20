package s7i.flink.meshcore.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TextMessage implements Serializable {

    private String from;
    private String message;
}
