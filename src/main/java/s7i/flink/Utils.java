package s7i.flink;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Utils {

    public static Stream<String> split(String csv) {
        if (csv == null) {
            return Stream.empty();
        }
        return Arrays.stream(csv.split("\\,"))
              .filter(Predicate.not(String::isEmpty))
              .map(String::trim);
    }

    public static List<String> splitAsList(String s) {
        return split(s).collect(Collectors.toList());
    }
}
