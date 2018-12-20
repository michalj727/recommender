import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class Recommendations {

    private Connection connReport;
    private Connection connMip;
    private UniqueIds uniqIds;
    private FileIO fileIo;
    private String x;
    private Int2ObjectOpenHashMap<Int2IntOpenHashMap> mm = new Int2ObjectOpenHashMap<>();

    public static void main(String[] args) {
        System.out.println("starting...");
        try {
            new Recommendations("d:\\temp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Recommendations(String x) throws Exception {
        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");
        this.x=x;
        fileIo = new FileIO("d:\\temp");
        uniqIds = new UniqueIds(connReport);
        initMatrix();
        for (int xx = 2016; xx <= 2018; xx++) {
            ArrayList<String> regSeq = new ArrayList<>();
            fileIo.loadFile(xx, regSeq);
            System.out.println("year " + xx+" found "+regSeq.size());
            createMatrix(regSeq);
        }
        flushRecommendations();
        statRecommendations();

    }

    public void createMatrix(ArrayList<String> regSeq ) {
        try {
            for (String reg : regSeq) {
                String[] tt = reg.split(",");
                int[] reqTab = new int[tt.length];
                for (int i = 0; i < tt.length; i++) {
                    try {
                        reqTab[i] = Integer.parseInt(tt[i]);
                    } catch (NumberFormatException e) {
                        System.out.println(reg);
                        reqTab[i]=1;
                    }
                }
                for (int i = 0; i < reqTab.length; i++) {
                    for (int j = 0; j < reqTab.length; j++) {
                        if (reqTab[i] != reqTab[j]) {
                            mm.get(reqTab[i]).put(reqTab[j], mm.get(reqTab[i]).get(reqTab[j]) + 1);
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
            mm.put(i, new Int2IntOpenHashMap());
        }
    }

    public void flushRecommendations() {
        System.out.println("init flushing");
        int x = 0;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        try {
            for (int i : mm.keySet()) {
                if (x++ % (mm.keySet().size() / 10) == 0) {
                    System.out.println("["+ formatter.format(new java.util.Date()) +" "+ (100 * x  / mm.keySet().size()) + "%]");
                }
                Int2IntOpenHashMap col = mm.get(i);
                for (int j : col.keySet()) {
                    if (col.get(j)>1) {
                        String[] id1 = uniqIds.get1(i).split(":");
                        String sql = "insert into related_user_based_recom(zobjectid, zpozycjaapid, addr, value) values("+id1[0]+","+id1[1]+",'"+uniqIds.legalisIds.get(j)+"',"+col.get(j)+")";
                        try {
                            connMip.prepareStatement(sql).executeUpdate();
                        } catch (SQLException e) {
                            //juz byl
                        }
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
        int k = 0;
        int[] xx = new int[30];
        int x = mm.keySet().size();

        try {
            for (int i : mm.keySet()) {
                Int2IntOpenHashMap col = mm.get(i);
                for (int j : col.keySet()) {
                    if (col.get(j)>0) {
                        k++;
                        if (col.get(j)<30) {
                            xx[col.get(j)-1]++;
                        } else {
                            xx[29]++;
                        }

                    }
                }
            }
            System.out.println("elements: "+k);
            for (int i=0; i<30; i++) {
                System.out.println((i+1)+" -> "+xx[i]);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection connecter(String user, String password, String db_name) throws
            Exception, SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connUrl="jdbc:sqlserver://mipdbserver.adbeck.pl;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }
}
