import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class ContentCorrelations {
    private final int numberOfArticlesFromOneIssue = 2;
    private Connection connMip;
    public final Long2ObjectOpenHashMap<String> correlated = new Long2ObjectOpenHashMap<String>();
    public final Long2LongOpenHashMap rootBooks = new Long2LongOpenHashMap();
    private TJIndex tj;
    private UniqueIds ids;


    public ContentCorrelations(UniqueIds ids, Connection connMip) throws Exception {
        System.out.println("loading correlations");
        this.ids=ids;
        this.connMip=connMip;

        tj = new TJIndex(connMip);

        PreparedStatement st = this.connMip.prepareStatement("select objectid, seria, rok, numer from litbeck where root='Czasopismo.Artykul'");
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            correlated.put(ids.ids.getLong(rs.getInt(1)+":0"), rs.getString(2)+"@"+rs.getString(3)+"@"+rs.getString(4));
        }

        st = this.connMip.prepareStatement("select doobjectid, zobjectid from objobj obj1 where objectzwiazektypid=5");
        rs = st.executeQuery();
        while (rs.next()) {
            rootBooks.put(ids.ids.getLong(rs.getInt(2)+":"+rs.getInt(1)), ids.ids.getLong(rs.getInt(2)));
        }

        st = this.connMip.prepareStatement("select distinct zobjectid from objobj obj1 where objectzwiazektypid=5");
        rs = st.executeQuery();
        while (rs.next()) {
            rootBooks.put(ids.ids.getLong(rs.getInt(1)+":0"), ids.ids.getLong(rs.getInt(1)));
        }
    }

    public String isCorrelatedDesc(int i, int i1) {
        if (isJournalCorrelated(i, i1)) {
            return "journal";
        } else if (isCommonRoot(i,i1)) {
            return "root";
        } else if (isCommonArticle(i, i1)) {
            return "article";
        } else {
            return "";
        }
    }


    public boolean isCorrelated(long i, long i1) {
        return isJournalCorrelated(i, i1)  || isCommonRoot(i,i1) || isCommonArticle(i, i1) ;
    }

    public boolean isJournalCorrelated(long i, long i1) {
        return correlated.containsKey(i) && correlated.containsKey(i1) && correlated.get(i).equals(correlated.get(i1));
    }

    public boolean isCommonArticle(long id, long id1) {
        if (ids.getPapid(id)==ids.getPapid(id1) && ids.getPapid(id)!=0) {
                return true;
        }
        return false;
    }

    public boolean isCommonRoot(long id, long id1) {
        if (ids.getProductId(id).equals(ids.getProductId(id1))) {
            return true;
        }
        return false;
    }

}
