

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Recommendation {
    @JsonProperty("itemsRecomm")
    private Set<ItemRecomm> itemsRecomm = new TreeSet<>((o1, o2) -> (o1.getNum() > o2.getNum()) ? -1 : (o1.getNum() < o2.getNum()) ? 1 : 0);

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonRootName("item")
    public static class ItemRecomm {
        @JsonProperty("legalisId")
        private String legalisId;
        @JsonProperty("productId")
        private String productId;
        @JsonProperty("documentType")
        private String documentType;
        @JsonProperty("num")
        private long num;
    }

    public static class ViewJson {
    }

}
