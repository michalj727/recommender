import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonRootName("click")
public class OneClick {
    @JsonIgnore
    private String legalisId;
    @JsonIgnore
    private long legalisId1;
    @JsonIgnore
    private long legalisId2;
    @JsonProperty("uniqueId")
    private long uniqueId;
    @JsonIgnore
    private long duration ;
    @JsonIgnore
    private long time ;
    @JsonProperty("export")
    private boolean export ;
    @JsonIgnore
    private String type ;
    @JsonIgnore
    private boolean seq;
    @JsonIgnore
    private int sessionId;

    @JsonIgnore
    public boolean isComm() {
        return type.equals("beck-pl-c")||type.equals("beck-pl-csys");
    }

    @JsonIgnore
    public boolean isOrz() {
        return type.equals("beck-pl-dec");
    }

}
