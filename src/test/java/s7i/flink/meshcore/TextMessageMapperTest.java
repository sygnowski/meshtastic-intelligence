package s7i.flink.meshcore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import s7i.flink.meshcore.model.TextMessage;

class TextMessageMapperTest {

    static class TextMapperTestImpl implements TextMessageMapper {
        final MeshcoreDecodedMapper tdm = new MeshcoreDecodedMapper();

        Optional<TextMessage> map(byte[] data) {
            return map(tdm.mapDecoded(data));
        }
    }

    @Test
    void test() throws IOException {

        var data = TextMessageMapperTest.class.getResourceAsStream("/decoded/msg01.json").readAllBytes();


        var textMessage = new TextMapperTestImpl().map(data).orElseThrow();

        System.out.println(textMessage);
        assertNotNull(textMessage);
    }


    @Test
    void testNotTextMessage() throws IOException {

        var data = TextMessageMapperTest.class.getResourceAsStream("/decoded/msg02.json").readAllBytes();

        assertThrows(NoSuchElementException.class ,() -> {
            new TextMapperTestImpl().map(data).orElseThrow();
        });
    }

}