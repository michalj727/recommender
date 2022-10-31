import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

public class Recommendations {

    private Connection connReport;
    private Connection connMip;
    private UniqueIds uniqIds;
    private FileIO fileIo;
    private String x;
    private Long2ObjectOpenHashMap<Long2LongOpenHashMap> mm = new Long2ObjectOpenHashMap<>();
    private ContentCorrelations coor;
    private int correlatedCount=0;



    Recommendations(String x) throws Exception {
        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");
        this.x=x;
        uniqIds = new UniqueIds(connReport, connMip);
        coor = new ContentCorrelations(uniqIds, connMip);
        fileIo = new FileIO("c:\\temp");
        initMatrix();
        for (int xx = 2018; xx <= 2022; xx++) {
            ObjectMapper objectMapper = new ObjectMapper();
            List<LogicalSession> sessionsList = objectMapper.readValue(new File("c://temp//seq"+xx+".json"), new TypeReference<List<LogicalSession>>() {});
            System.out.println("year " + xx+" found "+sessionsList.size());
            createMatrix(sessionsList);
        }
        flushRecommendations();
        statRecommendations();

    }

    public static void main(String[] args) {
        try {
            System.out.println("starting recommendations...");
            new Recommendations("d:\\temp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createMatrix(List<LogicalSession> x) {
        try {
            for (LogicalSession session : x) {
                long[] reqTab = new long[session.getClicks().size()];
                int i = 0;

                for (OneClick reg : session.getClicks()) {
                    reqTab[i++] = reg.getUniqueId();
                }

                for (i = 0; i < reqTab.length; i++) {
                    for (int j = 0; j < reqTab.length; j++) {
                        if (reqTab[i] != reqTab[j]) {
                            boolean correlated = coor.isCorrelated(reqTab[i], reqTab[j]);
                            if (!correlated) {
                                mm.get(reqTab[i]).put(reqTab[j], mm.get(reqTab[i]).get(reqTab[j]) + 1);
                            } else {
                                correlatedCount++;
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initMatrix() {
        System.out.println("init matrix");
        for (int i = 0; i < uniqIds.legalisIds.size(); i++) {
            mm.put(i, new Long2LongOpenHashMap());
        }
    }

    public void flushRecommendations() {

        System.out.println("init flushing");
        int x = 0;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        try {
            for (long i : mm.keySet()) {
                if (x++ % (mm.keySet().size() / 10) == 0)  {
                    System.out.println("["+ formatter.format(new java.util.Date()) +" "+ (100 * x  / mm.keySet().size()) + "%]");
                }
                Long2LongOpenHashMap col = mm.get(i);
                Recommendation recs = new Recommendation();
                ObjectMapper objectMapper = new ObjectMapper();
                for (long j : col.keySet()) {
                    if (col.get(j)>1) {
                        recs.getItemsRecomm().add(new Recommendation.ItemRecomm(uniqIds.legalisIds.get(j), uniqIds.productIds.get(j), uniqIds.documentTypes.get(j), col.get(j)));
                    }
                }
                StringWriter xx = new StringWriter();
                objectMapper.writeValue(xx, recs);
                if (recs.getItemsRecomm().size()>0) {
                    String sql = "insert into related_user_based_recom(zobjectid, zpozycjaapid, recomms) values(" + uniqIds.getBookid(i) + "," + uniqIds.getPapid(i) + ",'" + xx.toString() + "')";
                    try {
                        connMip.prepareStatement(sql).executeUpdate();
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
            System.out.println("done");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void statRecommendations() {
        System.out.println("statistics");
        System.out.println("correlated: "+correlatedCount);

    }

    public Connection connecter(String user, String password, String db_name) throws
            Exception, SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connUrl="jdbc:sqlserver://mipdbserver.adbeck.pl;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }
}
