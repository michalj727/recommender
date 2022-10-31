import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TJIndex {
    private final Long2LongOpenHashMap pap2ancestor = new Long2LongOpenHashMap();
    private final Long2LongOpenHashMap act2ancestor = new Long2LongOpenHashMap();
    Connection fc;

    public TJIndex(Connection fc) throws Exception {
        this.fc = fc;
        load();
    }

    private void load() throws Exception {
        PreparedStatement lst = fc.prepareStatement("select distinct id, ancestorid from pozycjaap where not ancestorid is null");
        ResultSet lrs = lst.executeQuery();
        while (lrs.next()) {
            pap2ancestor.put(lrs.getInt(1), lrs.getInt(2));
        }

        lst = fc.prepareStatement("select distinct ap1.id, aktprawny.id from aktprawny ap1, aktprawny where ap1.puuchid=aktprawny.puuchid and not ap1.putjid is null and aktprawny.putjid is null");
        lrs = lst.executeQuery();
        while (lrs.next()) {
            act2ancestor.put(lrs.getInt(1), lrs.getInt(2));
        }
    }

    public long getAncestor(long x) {
        return pap2ancestor.containsKey(x) ? pap2ancestor.get(x) : -1;
    }

    public long getAncestorOrSelf(long x) {
        return pap2ancestor.containsKey(x) ? pap2ancestor.get(x) : x;
    }


    public long getAncestorAct(long x) {
        return act2ancestor.containsKey(x) ? act2ancestor.get(x) : -1;
    }

    public long getAncestorActOrSelf(long x) {
        return act2ancestor.containsKey(x) ? act2ancestor.get(x) : x;
    }
}
