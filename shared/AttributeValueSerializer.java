package shared;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class AttributeValueSerializer {

    String serialize(AttributeValue av) {
        return switch (av.type()) {
            case null                    -> "";
            case S                       -> av.s();
            case N                       -> av.n();
            case BOOL                    -> av.bool().toString();
            case NUL                     -> "";
            case L                       -> toList(av.l());
            case M                       -> toMap(av.m());
            case SS                      -> toJsonStringArray(av.ss());
            case NS                      -> toJsonStringArray(av.ns());
            case B                       -> Base64.getEncoder().encodeToString(av.b().asByteArray());
            case BS                      -> toJsonBase64Array(av.bs());
            case UNKNOWN_TO_SDK_VERSION  -> "";
        };
    }

    private String toList(List<AttributeValue> list) {
        var sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            sb.append(toTyped(list.get(i)));
            if (i < list.size() - 1) sb.append(", ");
        }
        return sb.append("]").toString();
    }

    private String toMap(Map<String, AttributeValue> map) {
        var sb = new StringBuilder("{");
        var it = map.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            sb.append(" \"").append(e.getKey()).append("\" : ").append(toTyped(e.getValue()));
            if (it.hasNext()) sb.append(",");
        }
        return sb.append(" }").toString();
    }

    private String toTyped(AttributeValue av) {
        return switch (av.type()) {
            case null                    -> "null";
            case S                       -> "{ \"S\" : \"" + av.s().replace("\"", "\\\"") + "\" }";
            case N                       -> "{ \"N\" : \"" + av.n() + "\" }";
            case BOOL                    -> "{ \"BOOL\" : " + av.bool() + " }";
            case NUL                     -> "{ \"NULL\" : true }";
            case L                       -> "{ \"L\" : " + toList(av.l()) + " }";
            case M                       -> "{ \"M\" : " + toMap(av.m()) + " }";
            case SS                      -> "{ \"SS\" : " + toJsonStringArray(av.ss()) + " }";
            case NS                      -> "{ \"NS\" : " + toJsonStringArray(av.ns()) + " }";
            case B                       -> "{ \"B\" : \"" + Base64.getEncoder().encodeToString(av.b().asByteArray()) + "\" }";
            case BS                      -> "{ \"BS\" : " + toJsonBase64Array(av.bs()) + " }";
            case UNKNOWN_TO_SDK_VERSION  -> "null";
        };
    }

    private String toJsonStringArray(List<String> values) {
        var sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            sb.append("\"").append(values.get(i).replace("\"", "\\\"")).append("\"");
            if (i < values.size() - 1) sb.append(", ");
        }
        return sb.append("]").toString();
    }

    private String toJsonBase64Array(List<SdkBytes> values) {
        var sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            sb.append("\"").append(Base64.getEncoder().encodeToString(values.get(i).asByteArray())).append("\"");
            if (i < values.size() - 1) sb.append(", ");
        }
        return sb.append("]").toString();
    }
}
