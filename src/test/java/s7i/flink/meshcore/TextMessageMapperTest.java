package s7i.flink.meshcore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import s7i.flink.io.Packet;

class TextMessageMapperTest {


    @Test
    void test() throws IOException {

        var data = TextMessageMapperTest.class.getResourceAsStream("/decoded/msg01.json").readAllBytes();


        var tm = new MeshcoreDecodedMapper().mapAsOptional(data).orElseThrow();

        assertNotNull(tm);
    }


//    @Test
//    void testNotTextMessage() throws IOException {
//
//        var data = TextMessageMapperTest.class.getResourceAsStream("/decoded/msg02.json").readAllBytes();
//
//        assertThrows(NoSuchElementException.class ,() -> {
//            new MeshcoreDecodedMapper().mapAsOptional(data).orElseThrow();
//        });
//    }

}