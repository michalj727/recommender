import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.FileWriter;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SearchAnalizerLoader {

    Connection connReport;
    Connection connMip;

    public static void main(String[] args) {
        try {
            new SearchAnalizerLoader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    SearchAnalizerLoader() throws Exception {

        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");

        HashMap<String,  Integer> facets = new HashMap<>();
        String sql = " from action, log_sesja where type='search' and sessionId=log_sesja.id ";
        String order = " order by uzytkownikid, sessionId, timestamp ";
        String select = "select documentId, productId, timestamp, sessionId, uzytkownikid, searchQuery, searchFields ";

        PreparedStatement st = connReport.prepareStatement(select + sql + order);
        System.out.println(select + sql + order);
        ResultSet rs = st.executeQuery();
        processRows(facets, rs);

        FileWriter writer = new FileWriter("c:\\temp\\fs.txt");
        for (String key : facets.keySet()) {
            writer.write(key+";"+facets.get(key)+"\n");
        }
        writer.close();
    }



    public void processRows(Map<String, Integer> mapCount, ResultSet rs) throws Exception {
        int currSessionId = -1;
        String currUser=null;
        long milisecs = -1;
        int i = 0;
        long duration=-1;
        int sessions = 0;

        IntArrayList corr = new IntArrayList();
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        List<LogicalSession> sessionsShortList = new ArrayList<>();


        while (rs.next()) {
            try {
                long currStamp = rs.getDate("timestamp").getTime() + rs.getTime("timestamp").getTime();
                currUser = rs.getString("uzytkownikid");
                if (currSessionId==-1) {
                    currSessionId = rs.getInt("sessionId");
                    sessions++;
                    sessionsShortList.add(new LogicalSession(true, currSessionId, currUser, new ArrayList<>()));
                } else if (currSessionId != rs.getInt("sessionId")) {
                    //true new session
                    currSessionId = rs.getInt("sessionId");
                    sessionsShortList.add(new LogicalSession(true, currSessionId, currUser, new ArrayList<>()));
                    sessions++;
                }
                duration = currStamp - milisecs;
                milisecs = currStamp;

                String adrrDoc = rs.getString("documentId").toUpperCase();
                String adrrProd = rs.getString("productId").toUpperCase();
                String query = rs.getString("searchQuery");
                String fields = rs.getString("searchFields");
                try {
                    //## $facets=$document-type$/Orzeczenia ## $facets=$court$/Sąd Najwyższy
                    List<String> xx = new ArrayList<>();
                    String xxx = "";
                    if (fields.contains("facets")) {
                        String[] fieldsTab = fields.split("##");
                        for (int j = 0; j < fieldsTab.length; j++) {
                            if (fieldsTab[j].contains("$facets")) {
                                if (fieldsTab[j].contains("document-type")) {
                                    xxx = fieldsTab[j].replace("$facets=", "").trim();
                                } else {
                                    xx.add(fieldsTab[j].replace("$facets=", "").trim());
                                }
                            } else if (fieldsTab[j].contains("_sf-doc-date-")) {
                                xx.add(fieldsTab[j].replace("_sf-", "").trim());
                            }
                        }
                        java.util.Collections.sort(xx);
                        xxx += ";";
                        xxx += xx.stream().collect(Collectors.joining("->"));
                        if (!mapCount.containsKey(xxx)) {
                            mapCount.put(xxx, 0);
                        }
                        mapCount.put(xxx, mapCount.get(xxx) + 1);
                    }
                } catch (Exception e) {
                    System.out.println("err " + adrrDoc + ", " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public Connection connecter(String user, String password, String db_name) throws
            Exception, SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connUrl="jdbc:sqlserver://mipdbserver.adbeck.pl;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }

}
