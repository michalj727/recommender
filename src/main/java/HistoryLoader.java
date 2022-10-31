import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class HistoryLoader {

    private final String cTypes = "documentType in ('beck-pl-analiza','beck-pl-bib','beck-pl-c','beck-pl-csys','beck-pl-infor','beck-pl-ius','beck-pl-jour','beck-pl-kierunek','beck-pl-mono','beck-pl-news','beck-pl-nius','beck-pl-porad','beck-pl-rz','beck-pl-sys')";
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

        fileIo = new FileIO("c:\\temp");
        uniqIds = new UniqueIds(connReport, connMip);

        //for (int xx = 2016; xx <= 2018; xx++) {
        for (int xx = 2021; xx <= 2021; xx++) {
            System.out.println("year "+xx);
            String sql = " from action_test where "+cTypes+" and timestamp>'2021-9-9' ";
            if (xx<2018) {
                sql = " from get_history4 where "+cTypes+" and year(timestamp)="+xx;
            }
            String order = " order by sessionId, timestamp";
            String select = "select documentId, productId, timestamp, sessionId, type, documentType  ";

            //PreparedStatement st = connReport.prepareStatement("select count(*) "+sql);
            //ResultSet rs = st.executeQuery();
            //rs.next();
            //int x = rs.getInt(1);
            System.out.println(select+sql+order);
            PreparedStatement st = connReport.prepareStatement(select+sql+order);
            ResultSet rs = st.executeQuery();

            processRows(Integer.MAX_VALUE, rs);
            //fileIo.createFile(xx, regSeq);

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


        LongArrayList corr = new LongArrayList();
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");


        while (rs.next()) {
            try {
                if (i++ % (xx / 10) == 0) {
                    System.out.println("["+ formatter.format(new java.util.Date()) +" "+ (100 * i  / xx) + "%]");
                }
                long currStamp = rs.getDate("timestamp").getTime();
                boolean skipLast = false;
                if (currSessionId == -1) {
                    currSessionId = rs.getInt("sessionId");
                    sessions++;
                } else if (currSessionId == rs.getInt("sessionId")) {
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
                    currSessionId = rs.getInt("sessionId");
                    sessions++;
                }
                milisecs = currStamp;


                String adrrDoc = rs.getString("documentId").toUpperCase();
                String adrrProd = rs.getString("productId").toUpperCase();


                try {
                    long uniqId = 0;
                    boolean typeExport= false;
                    uniqId = uniqIds.get(adrrDoc, adrrProd, rs.getString("documentType"));
                    typeExport = isExport(rs);

                    if (!skipLast) {
                        addCoor(corr, uniqId, typeExport);
                    } else {
                        addCoorSkipLast(corr, uniqId, typeExport);
                    }

                } catch (Exception e) {
                    System.out.println("err "+adrrDoc+", "+e.getMessage());
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



    public void flushCoor(LongArrayList corr) {
        if (uniqIds.lastCoor>-1) {
            corr.add(uniqIds.lastCoor);
        }

        registerSeq(corr);

        corr.clear();
        uniqIds.lastCoor=-1;
        uniqIds.exportsInSeq=0;
    }

    public void registerSeq(LongArrayList corr) {
        if (corr.size()>registerSeqTresh) {
            if (uniqIds.exportsInSeq<exportsInSeqTresh) {
                StringBuffer buf = new StringBuffer();
                for (Long xx : corr) {
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

    public void addCoor(LongArrayList corr, long pId, boolean export) {
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

    public void addCoorSkipLast(LongArrayList corr, long pId, boolean export) {
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
