import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class HistoryLoader {

    Connection connReport;
    Connection connMip;

    final int sessionTreshold=600000;
    final int shortClick=5000;
    final int registerSeqTresh=3;
    final int exportsInSeqTresh=30;

    int sessions=0;
    int toFewDocuments=0;
    int toMuchExports=0;
    int sessionBreaked=0;
    int toShort=0;

    ArrayList<String> regSeq = new ArrayList<String>();
    UniqueIds uniqIds;
    FileIO fileIo;


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

        fileIo = new FileIO("d:\\temp");
        uniqIds = new UniqueIds(connReport);


        for (int xx = 2016; xx <= 2018; xx++) {
            System.out.println("year "+xx);
            String sql = " from get where request_type_id=8 and year(date)="+xx;
            if (xx<2018) {
                sql = " from get_history4 where request_type_id=8 and year(date)="+xx;
            }
            String order = " order by session_id, date";
            String select = "select id1, id2, date, session_id, type, document ";

            PreparedStatement st = connReport.prepareStatement("select count(*) "+sql);
            ResultSet rs = st.executeQuery();
            rs.next();
            int x = rs.getInt(1);
            st = connReport.prepareStatement(select+sql+order);
            rs = st.executeQuery();

            processRows(x, rs);
            fileIo.createFile(xx, regSeq);

            System.out.println("sessions: "+sessions);
            System.out.println("related: "+regSeq.size());
            System.out.println("sessionBreaked: "+sessionBreaked);
            System.out.println("toShort: "+toShort);
            System.out.println("toFew: "+toFewDocuments);
            System.out.println("toMuchExports: "+toMuchExports);

            regSeq.clear();
            sessions=0;
            sessionBreaked=0;
            toShort=0;
            toFewDocuments=0;
            toMuchExports=0;
        }
    }

    public void processRows(int xx, ResultSet rs) throws Exception {
        int currSessionId=-1;
        long milisecs=-1;
        int i=0;


        IntArrayList corr = new IntArrayList();
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");


        while (rs.next()) {
            try {
                if (i++ % (xx / 10) == 0) {
                    System.out.println("["+ formatter.format(new java.util.Date()) +" "+ (100 * i  / xx) + "%]");
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
                    int uniqId = 0;
                    boolean typeExport= false;
                    int id1 = getId1(rs);
                    int id2= getId2(rs, id1);
                    String legalisName=calcLegalisName(adrr1, adrr2, id1, id2);
                    uniqId = uniqIds.get(id1, id2, legalisName);
                    typeExport = isExport(rs);

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
        try {
            if (rs.getString("type").startsWith("print") || rs.getString("type").startsWith("save")) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new Exception("exp: "+e.getMessage());
        }
    }

    private String calcLegalisName(String adrr1, String[] adrr2, int id1, int id2) throws Exception {
        try {
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
        } catch (Exception e) {
            throw new Exception("calcLegalisName: "+null);
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

}
