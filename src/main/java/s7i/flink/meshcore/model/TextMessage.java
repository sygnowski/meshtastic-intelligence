package s7i.flink.meshcore.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import s7i.flink.Utils;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TextMessage implements Serializable {

    private String messageHash;
    private String channelHash;
    private Long timestamp;
    private String from;
    @Builder.Default
    private List<List<String>> pathList = new ArrayList<>();
    private String text;

    public TextMessage addPath(List<String> path) {
        pathList.add(new ArrayList<>(path));
        return this;
    }

    public String title() {
        return from
                + " on channel: "
                + channelHash;
    }

    public String message() {
        return "Path: "
                + path()
                + "Time: " + Utils.fromEpoch(timestamp)
                + "\n"
                + text;
    }

    private String path() {
        if (pathList.size() == 1) {
            return String.join("->", pathList.stream().findFirst().orElseThrow()) + "\n";
        } else if (pathList.size() > 1) {
            var sb = new StringBuilder("\n");
            int ln = 0;
            for (var p : pathList) {
                sb.append( ++ln).append(": ").append(String.join("->", p)).append("\n");
            }
            return sb.toString();
        }
        return "<no path>";
    }

    public TextMessage asPretty() {
        return TextMessage.builder()
                .from(title())
                .text(message())
                .build();
    }
}
