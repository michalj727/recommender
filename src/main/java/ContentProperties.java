import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ContentProperties {
    private Connection connMip;
    public final Int2IntOpenHashMap comSize = new Int2IntOpenHashMap();


    public ContentProperties(Connection connMip) throws Exception {
        System.out.println("loading correlations");
        this.connMip=connMip;

        PreparedStatement st = this.connMip.prepareStatement("select objectid, count(*) from kompap, pozycjaap where pozycjaapid=pozycjaap.id and pozycjaaplicznik=pozycjaap.licznik and tagxmlid in (8, 9, 1002, 1004) group by objectid");
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            comSize.put(rs.getInt(1), rs.getInt(2));
        }
    }


}
