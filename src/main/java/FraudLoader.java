import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.File;
import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FraudLoader {

    Connection connReport;
    Connection connMip;
    private final String cTypes = "documentType in ('beck-pl-c')";

    int sessions = 0;

    List<String> whole = new ArrayList<String>();
    UniqueIds uniqIds;
    ContentCorrelations coor;
    ContentProperties commentProperties;
    FileIO fileIo;



    public static void main(String[] args) {
        try {
            new FraudLoader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    FraudLoader() throws Exception {

        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");

        fileIo = new FileIO("c:\\temp");
        uniqIds = new UniqueIds(connReport, connMip);
        coor = new ContentCorrelations(uniqIds, connMip);
        commentProperties = new ContentProperties(connMip);


        List<LogicalSession> sessionsList = new ArrayList<>();
        String sql = " from action, log_sesja where "+cTypes+" and sessionId=log_sesja.id and timestamp>'2021-09-01'";
        String order = " order by uzytkownikid, sessionId, timestamp ";
        String select = "select documentId, productId, timestamp, sessionId, documentType, type, uzytkownikid ";

        PreparedStatement st = connReport.prepareStatement(select + sql + order);
        ResultSet rs = st.executeQuery();
        processRows(sessionsList, rs);
        whole.forEach(x -> {
            try {
                String[] y = x.split(";");
                PreparedStatement st1 = connReport.prepareStatement("insert into bad_users(username, sessions, book, articles, started) " +
                        "values(?,?,?,?,?)");

                st1.setString(1,y[0]);
                st1.setInt(2, Integer.parseInt(y[1].trim()));
                st1.setInt(3, Integer.parseInt(y[2].trim()));
                st1.setInt(4, Integer.parseInt(y[3].trim()));
                st1.setDate(5 , new Date(Long.parseLong(y[5].trim())));
                st1.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    private void filterRows(List<LogicalSession> sessionsShortList) {
        Map<String, List<LogicalSession>> userBased = sessionsShortList.stream().collect(Collectors.groupingBy(LogicalSession::getUserId));
        for (String y : userBased.keySet()) {
            //all for assumed user
            List<OneClick> all4User = userBased.get(y).stream().flatMap(yy -> yy.getClicks().stream()).collect(Collectors.toList());
            //remove all views of the same page during week period
            all4User = all4User.stream().filter(distinctByKey(x-> (x.getLegalisId1()+"@"+x.getLegalisId2()))).collect(Collectors.toList());
            markSuspectedSeq(all4User, y);
        }
    }


    private void markSuspectedSeq(List<OneClick> x, String sessionId) {
        //for each commentary count views
        Map<Long, Long> count = x.stream().filter(y -> (y.isComm() && (y.isExport()))).collect(Collectors.groupingBy(y -> y.getLegalisId1(), Collectors.counting()));

        for (Long book : count.keySet()) {
            //elements in this commentary
            double artInKom = (double) commentProperties.comSize.get(book.intValue());
            //how many sessions has occured during browsing commentary
            List<Integer> sesionsUsed = x.stream().filter(y -> y.getLegalisId1() == book.intValue()).map(OneClick::getSessionId).distinct().collect(Collectors.toList());

            Optional<Long> stamp  = x.stream().filter(y -> y.getLegalisId1() == book.intValue()).map(OneClick::getTime).findAny();

            double elementsOpened = (double) count.get(book);
            double percentOpened = elementsOpened / artInKom;

            if (artInKom > 30 && (elementsOpened > 100 || percentOpened > 0.75)) {
                //whole books downloaded
                logBook(whole, sessionId, sesionsUsed.size(), book, elementsOpened, percentOpened, stamp.get());
            }
        }
    }

    private void logBook(List<String> log, String sessionId, int sessionsUsed, long bookId, double elemsOpened, double percentOpened, long currTime) {
        log.add(sessionId + "; "+sessionsUsed+"; " + bookId + "; " + (int)Math.ceil(elemsOpened) + "; " + percentOpened + "; " + currTime);
    }



    public void processRows(List<LogicalSession> sessionsList, ResultSet rs) throws Exception {
        int currSessionId = -1;
        String currUser=null;
        long milisecs = -1;
        int i = 0;
        long duration=-1;

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

                try {
                    long uniqId = 0;
                    boolean typeExport = false;
                    uniqId = uniqIds.get(adrrDoc, adrrProd, rs.getString("documentType"));
                    typeExport = isExport(rs);
                    LogicalSession logicalSession = sessionsShortList.get(sessionsShortList.size() - 1);

                    if (typeExport && logicalSession != null && logicalSession.getClicks().size()>0) {
                        logicalSession.getClicks().get(logicalSession.getClicks().size() - 1).setExport(true);
                    } else if (typeExport && logicalSession != null && logicalSession.getClicks().size()==0) {
                        //export opening session
                    } else {
                        OneClick y = new OneClick(adrrDoc, uniqIds.getBookid(uniqId), uniqIds.getPapid(uniqId), uniqId, duration, currStamp, typeExport, rs.getString("documentType"), false, currSessionId);
                        logicalSession.getClicks().add(y);
                    }

                } catch (Exception e) {
                    System.out.println("err " + adrrDoc + ", " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        filterRows(sessionsShortList);
        sessionsList.addAll(sessionsShortList);
        sessionsShortList.clear();
    }


    private boolean isExport(ResultSet rs) throws Exception {
        try {
            if (rs.getString("type").startsWith("print") || rs.getString("type").startsWith("save")) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new Exception("exp: " + e.getMessage());
        }
    }


    public Connection connecter(String user, String password, String db_name) throws
            Exception, SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connUrl="jdbc:sqlserver://mipdbserver.adbeck.pl;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }

}
