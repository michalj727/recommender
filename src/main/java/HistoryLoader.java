import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.ArrayList;

public class HistoryLoader {
    private class UniqueIds {

        public final Object2IntOpenHashMap<String> ids = new Object2IntOpenHashMap<String>();
        public final Int2ObjectOpenHashMap<String> ids1 = new Int2ObjectOpenHashMap<String>();
        public final Int2ObjectOpenHashMap<String> legalisIds = new Int2ObjectOpenHashMap<String>();
        public int lastCoor=-1;
        public int exportsInSeq=0;
        private int maxId=-1;

        public UniqueIds() throws Exception {
            System.out.println("loading ids");
            PreparedStatement st = connReport.prepareStatement("select id1, id2, uniqId, legalisName from booksids order by uniqId desc");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                if (maxId==-1) {
                    maxId=rs.getInt(3);
                }
                ids.put(rs.getInt(1)+":"+rs.getInt(2), rs.getInt(3));
                ids1.put(rs.getInt(3), rs.getInt(1)+":"+rs.getInt(2));
                legalisIds.put(rs.getInt(3), rs.getString(4).trim());
            }
            System.out.println("ids done");
        }

        public int addItem(int id1, int id2, String legalisName) throws Exception {
            maxId++;
            connReport.prepareStatement("insert into booksids(id1, id2, legalisName, uniqId) values("+id1+","+id2+",'"+legalisName+"',"+maxId+")").executeUpdate();
            ids.put(id1+":"+id2, maxId);
            ids1.put(maxId, id1+":"+id2);
            legalisIds.put(maxId, legalisName.trim());
            return maxId;
        }

        public void save() {
            try {
                FileChannel rwChannel = new RandomAccessFile("d:\\temp\\yy.txt", "rw").getChannel();

                for (Integer i : legalisIds.keySet()) {
                    String out = i+","+legalisIds.get(i)+"\n";
                    ByteBuffer buf = ByteBuffer.allocate(out.length());
                    buf.clear();
                    buf.put(out.getBytes());
                    buf.flip();
                    rwChannel.write(buf);
                }
                rwChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public int get(int id1, int id2, String legalisName) throws Exception {
            String xx = id1+":"+id2;
            if (!ids.containsKey(xx)) {
                addItem(id1, id2, legalisName.trim());
            }
            return ids.getInt(xx);
        }

        public String get1(int id) throws Exception {
            return ids1.get(id);
        }
    }


    Connection connReport;
    Connection connMip;

    final int sessionTreshold=600000;
    final int shortClick=5000;
    final int registerSeqTresh=4;
    final int exportsInSeqTresh=50;
    public int startYear=2017;

    int sessions=0;
    int toFewDocuments=0;
    int toMuchExports=0;
    int sessionBreaked=0;
    int toShort=0;

    ArrayList<String> regSeq = new ArrayList<String>();
    UniqueIds uniqIds;
    Int2ObjectOpenHashMap<Int2IntOpenHashMap> mm = new Int2ObjectOpenHashMap<>();



    public static void main(String[] args) {
        System.out.println("starting...");
        try {
            new HistoryLoader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    HistoryLoader() throws Exception {

        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");

        uniqIds = new UniqueIds();

        String sql = " from get where request_type_id=8 and date>CONVERT(datetime, '01/01/"+startYear+"', 103)";
        String order = " order by session_id, date";
        String select = "select id1, id2, date, session_id, type, document ";

        PreparedStatement st = connReport.prepareStatement("select count(*) "+sql);
        ResultSet rs = st.executeQuery();
        rs.next();
        int x = rs.getInt(1);
        st = connReport.prepareStatement(select+sql+order);
        rs = st.executeQuery();

        processRows(x, rs);
        new CreateFile();
        uniqIds.save();

        System.out.println("sessions: "+sessions);
        System.out.println("related: "+regSeq.size());
        System.out.println("sessionBreaked: "+sessionBreaked);
        System.out.println("toShort: "+toShort);
        System.out.println("toFew: "+toFewDocuments);
        System.out.println("toMuchExports: "+toMuchExports);

        createMatrix();
        flushRecommendations();
    }

    public void processRows(int xx, ResultSet rs) throws Exception {
        int currSessionId=-1;
        long milisecs=-1;
        int i=0;


        IntArrayList corr = new IntArrayList();

        while (rs.next()) {
            try {
                if (i++ % 100000 == 0) {
                    System.out.println("["+ String.format("%.3f",(double) i  / xx) + "]");
                }
                long currStamp = rs.getDate("date").getTime();
                boolean skipLast = false;
                if (currSessionId == -1) {
                    currSessionId = rs.getInt("session_id");
                    sessions++;
                } else if (currSessionId == rs.getInt("session_id")) {
                    if (currStamp - milisecs < sessionTreshold) {
                        if ((currStamp - milisecs < shortClick)) {
                            skipLast = true;
                            toShort++;
                        }
                    } else {
                        flushCoor(corr);
                        sessionBreaked++;
                    }
                } else {
                    flushCoor(corr);
                    currSessionId = rs.getInt("session_id");
                    sessions++;
                }
                milisecs = currStamp;


                String adrr1 = rs.getString("document").toUpperCase();
                String[] adrr2 = adrr1.split("\\.");
                try {
                    int id1 = getId1(rs);
                    int id2= getId2(rs, id1);
                    String legalisName=calcLegalisName(adrr1, adrr2, id1, id2);
                    int uniqId = uniqIds.get(id1, id2, legalisName);
                    boolean typeExport=isExport(rs);


                    if (!skipLast) {
                        addCoor(corr, uniqId, typeExport);
                    } else {
                        addCoorSkipLast(corr, uniqId, typeExport);
                    }
                } catch (Exception e) {
                    System.out.println("err "+adrr1+", "+e.getMessage());
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private boolean isExport(ResultSet rs) throws Exception {
        if (rs.getString("type").startsWith("print") || rs.getString("type").startsWith("save")) {
            return true;
        }
        return false;
    }

    private String calcLegalisName(String adrr1, String[] adrr2, int id1, int id2) {
        if (adrr1.contains(".PAP.")) {
            if (adrr2.length==5) {
                return adrr2[0]+"."+adrr2[1]+".ACT."+adrr2[2]+"."+adrr2[3]+"."+adrr2[4];
            } else {
                return adrr1;
            }
        } else if (adrr2.length>2 && id2>-1) {
            return "BOOK."+id2;
        } else if (adrr2.length>2 && id2==-1) {
            return "BOOK."+id1;
        } else if (id1>-1 && id2>-1) {
            return "BOOK."+id2;
        } else {
            return "BOOK."+id1;
        }
    }

    private int getId2(ResultSet rs, int id1) throws Exception {
        rs.getString(2);
        if (!rs.wasNull()) {
            try {
                int i = rs.getInt(2);
                if (id1==i) {
                    return -1;
                }
                return i;
            } catch (Exception e) {

            }
        }
        return -1;
    }

    private int getId1(ResultSet rs) throws Exception {
        try {
             return rs.getInt(1);
        } catch (Exception e) {
            //205113645_srodtyt3
            return Integer.parseInt(rs.getString(1).substring(0, rs.getString(1).indexOf('_')-1));
        }
    }

    public void createMatrix() {
        int k = 0;

        System.out.println("init matrix");
        for (int i = 0; i < uniqIds.legalisIds.size(); i++) {
            mm.put(i, new Int2IntOpenHashMap());
        }

        try {
            for (String reg : regSeq) {
                String[] tt = reg.split(",");
                int[] reqTab = new int[tt.length];
                for (int i = 0; i < tt.length; i++) {
                    reqTab[i] = Integer.parseInt(tt[i]);
                }
                for (int i = 0; i < reqTab.length; i++) {
                    for (int j = 0; j < reqTab.length; j++) {
                        if (reqTab[i] != reqTab[j]) {
                            mm.get(reqTab[i]).put(reqTab[j], mm.get(reqTab[i]).get(reqTab[j]) + 1);
                        }
                    }
                }
            }
            System.out.println("done");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void flushRecommendations() {
        System.out.println("init flushing");
        int k = 0;
        int x = mm.keySet().size();

        try {
            for (int i : mm.keySet()) {
                Int2IntOpenHashMap col = mm.get(i);
                for (int j : col.keySet()) {
                    if (col.get(j)>1) {
                        //System.out.println("for "+uniqIds.legalisIds.get(i)+" -> "+uniqIds.legalisIds.get(j)+" x "+col.get(j));
                        String[] id1 = uniqIds.get1(i).split(":");
                        //System.out.println("insert into related_user_based_recom(zobjectid, zpozycjaapid, addr, value) values("+id1[0]+","+id1[1]+",\""+uniqIds.legalisIds.get(j)+"\","+col.get(j)+")");
                    }
                }
            }
            System.out.println("done");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void flushCoor(IntArrayList corr) {
        if (uniqIds.lastCoor>-1) {
            corr.add(uniqIds.lastCoor);
        }

        registerSeq(corr);

        corr.clear();
        uniqIds.lastCoor=-1;
        uniqIds.exportsInSeq=0;
    }

    public void registerSeq(IntArrayList corr) {
        if (corr.size()>registerSeqTresh) {
            if (uniqIds.exportsInSeq<exportsInSeqTresh) {
                StringBuffer buf = new StringBuffer();
                for (Integer xx : corr) {
                    buf.append(xx+",");
                }
                regSeq.add(buf.toString());
            } else {
                toMuchExports++;
            }
        } else {
            toFewDocuments++;
        }
    }

    public void addCoor(IntArrayList corr, int pId, boolean export) {
        if (uniqIds.lastCoor>-1) {
            corr.add(uniqIds.lastCoor);
        }
        uniqIds.lastCoor=pId;
        if (export) {
            corr.add(uniqIds.lastCoor);
            uniqIds.lastCoor=-1;
            uniqIds.exportsInSeq++;
        }
    }

    public void addCoorSkipLast(IntArrayList corr, int pId, boolean export) {
        uniqIds.lastCoor=pId;
        if (export) {
            corr.add(uniqIds.lastCoor);
            uniqIds.lastCoor=-1;
            uniqIds.exportsInSeq++;
        }
    }

    public Connection connecter(String user, String password, String db_name) throws
            Exception, SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connUrl="jdbc:sqlserver://mipdbserver.adbeck.pl;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }

    private class CreateFile {
        FileChannel rwChannel;

        public CreateFile() {
            try {
                rwChannel = new RandomAccessFile("d:\\temp\\xx.txt", "rw").getChannel();

                for (String reg : regSeq) {
                    String out = reg+"\n";
                    ByteBuffer buf = ByteBuffer.allocate(out.length());
                    buf.clear();
                    buf.put(out.getBytes());
                    buf.flip();
                    rwChannel.write(buf);
                }
                rwChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
