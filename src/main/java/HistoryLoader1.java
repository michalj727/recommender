import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HistoryLoader1 {

    Connection connReport;
    Connection connMip;
    private final String cTypes = "documentType in ('beck-pl-analiza','beck-pl-bib','beck-pl-c','beck-pl-csys','beck-pl-infor','beck-pl-ius','beck-pl-jour','beck-pl-kierunek','beck-pl-mono','beck-pl-news','beck-pl-nius','beck-pl-porad','beck-pl-rz','beck-pl-sys')";

    final int sessionTreshold = 1800000; //30min
    final int shortClick = 15000; //15s
    final int registerSeqTresh = 4;
    final int exportsInSeqTresh = 15;
    final int registerSeqTreshToLong = 100;
    final double toMuchShortTresh = 0.75;

    int sessions = 0;

    List<String> info = new ArrayList<String>();
    List<String> passed = new ArrayList<String>();
    List<String> downloads = new ArrayList<String>();
    List<String> whole = new ArrayList<String>();
    UniqueIds uniqIds;
    ContentCorrelations coor;
    ContentProperties commentProperties;
    FileIO fileIo;



    public static void main(String[] args) {

        try {
            String str = Arrays.stream(args).collect(Collectors.joining());
            if (str.contains("-hist")) {
                System.out.println("starting with history...");
                new HistoryLoader1();
            } else if (str.contains("-recomm")) {
                System.out.println("starting recommendations...");
                new Recommendations("c:\\temp");
            } else if (str.contains("-all")) {
                System.out.println("starting with history...");
                new HistoryLoader1();
                System.out.println("starting recommendations...");
                new Recommendations("c:\\temp");
            } else {
                System.out.println("avaliable options are -all - hist -recomm");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    HistoryLoader1() throws Exception {

        connReport = connecter("jaworskim", "jaworskim", "legalis_report");
        connMip = connecter("jaworskim", "jaworskim", "mip");

        fileIo = new FileIO("c:\\temp");
        uniqIds = new UniqueIds(connReport, connMip);
        coor = new ContentCorrelations(uniqIds, connMip);
        commentProperties = new ContentProperties(connMip);

        //int[] years ={2018};
        //String[] actions ={"action2018"};

        int[] years ={2022, 2021, 2020, 2019, 2018};
        String[] actions ={ "action", "action2021", "action2020", "action2019", "action2018"};


        for (int i = 0; i < years.length; i++) {
            List<LogicalSession> sessionsList = new ArrayList<>();
            int year = years[i];
            System.out.println("year " + year);
            String sql = " from "+actions[i]+", session where "+cTypes+" and sessionId=session.id and datepart(year , timestamp)="+year;
            String order = " order by datepart(week , timestamp), uzytkownikid, sessionId, timestamp ";
            String select = "select documentId, productId, timestamp, sessionId, documentType, type, session.userId as uzytkownikid, datepart(week , timestamp) as week   ";

            PreparedStatement st = connReport.prepareStatement(select + sql + order);
            ResultSet rs = st.executeQuery();
            processRows(sessionsList, rs);
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File("c://temp//seq"+year+".json"), sessionsList);
            sessions = 0;
        }

        //fileIo.createFile("info.txt", info);
        //fileIo.createFile("down.txt", downloads);
        //fileIo.createFile("whole.txt", whole);
        //fileIo.createFile("passed.txt", passed);
        whole.forEach(x -> {
            try {
                String[] y = x.split(";");
                PreparedStatement st = connReport.prepareStatement("insert into bad_users(username, sessions, book, articles, started) " +
                        "values(?,?,?,?,?)");

                st.setString(1,y[0]);
                st.setInt(2, Integer.parseInt(y[1].trim()));
                st.setInt(3, Integer.parseInt(y[2].trim()));
                st.setInt(4, Integer.parseInt(y[3].trim()));
                st.setDate(5 , new Date(Long.parseLong(y[5].trim())));
                st.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }


    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    private List<LogicalSession>filterRows(List<LogicalSession> sessionsShortList) {
        List<LogicalSession> xx;
        TreeSet<String> bannedUsers = new TreeSet<>();
        info.add("total sessions = "+sessionsShortList.size());
        //to few books opened
        xx = sessionsShortList.parallelStream().filter(x -> x.getClicks().size()>registerSeqTresh).collect(Collectors.toList());
        info.add("after to few = "+xx.size());

        Map<String, List<LogicalSession>> userBased = xx.stream().collect(Collectors.groupingBy(LogicalSession::getUserId));
        for (String y : userBased.keySet()) {
            //all for assumed user
            List<OneClick> all4User = userBased.get(y).stream().flatMap(yy -> yy.getClicks().stream()).collect(Collectors.toList());
            //remove all views of the same page during week period
            //TODO is it proper approach?
            all4User = all4User.stream().filter(distinctByKey(x-> (x.getLegalisId1()+"@"+x.getLegalisId2()))).collect(Collectors.toList());
            if (markSuspectedSeq(all4User, y)) {
                bannedUsers.add(y);
            }
        }
        //remove all sessions of banned users
        xx = xx.parallelStream().filter(y -> !bannedUsers.contains(y.getUserId())).collect(Collectors.toList());

        info.add("after banned users = "+xx.size());

        return xx;
    }

    private void toLong(LogicalSession x) {
        if (x.getClicks().size()>registerSeqTreshToLong) {
            System.out.println(x.getSessionId());
        }
    }

    private void removeShortClicks(LogicalSession x) {
        x.setClicks(x.getClicks().stream().filter(y -> y.getDuration()<shortClick).collect(Collectors.toList()));
    }

    private boolean markSuspectedSeq(List<OneClick> x, String sessionId) {
        //for each commentary count views
        Map<Long, Long> count = x.stream().filter(y -> y.isComm()).collect(Collectors.groupingBy(y -> y.getLegalisId1(), Collectors.counting()));
        boolean score = false;
        long lastId=-1;
        //mark subsequent views belonging to the same commentary
        for (OneClick y : x) {
            if (y.getLegalisId1()==lastId) {
                y.setSeq(true);
            } else {
                lastId=y.getLegalisId1();
            }
        }
        //mark suspectd views means in sequence or short or export
        Map<Long, Long> suspected = x.stream().filter(y -> y.isComm() && (y.getDuration()<5000) || y.isExport() || y.isSeq()).collect(Collectors.groupingBy(y -> y.getLegalisId1(), Collectors.counting()));


        for (Long book : count.keySet()) {
            //elements in this commentary
            double artInKom = (double) commentProperties.comSize.get(book.intValue());
            //how many sessions has occured during browsing commentary
            List<Integer> sesionsUsed = x.stream().filter(y -> y.getLegalisId1() == book.intValue()).map(OneClick::getSessionId).distinct().collect(Collectors.toList());

            Optional<Long> stamp  = x.stream().filter(y -> y.getLegalisId1() == book.intValue()).map(OneClick::getTime).findAny();

            double elementsOpened = (double) count.get(book);
            double suspectedInKom = (suspected.get(book)!=null ? (double) suspected.get(book) : 0.0) / elementsOpened;
            double percentOpened = elementsOpened / artInKom;

            if (elementsOpened > 30 && (percentOpened > 0.75) &&  (suspectedInKom > 0.75)) {
                //whole books downloaded
                logBook(whole, sessionId, sesionsUsed.size(), book, elementsOpened, percentOpened, stamp.get());
                score = true;
            } else if (elementsOpened > 10 && (suspectedInKom  > 0.75)) {
                //part of small commentary
                logBook(downloads, sessionId, sesionsUsed.size(), book, elementsOpened, percentOpened, stamp.get());
                score=true;
            } else {
                logBook(passed, sessionId, sesionsUsed.size(), book, elementsOpened, percentOpened, stamp.get());
            }
        }
        return score;
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

        int lastWeek = -1;
        IntArrayList corr = new IntArrayList();
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        List<LogicalSession> sessionsShortList = new ArrayList<>();


        while (rs.next()) {
            try {
                long currStamp = rs.getDate("timestamp").getTime() + rs.getTime("timestamp").getTime();
                currUser = rs.getString("uzytkownikid");
                if (lastWeek==-1) {
                    lastWeek=rs.getInt("week");
                    System.out.println("week: "+lastWeek);
                } else if (lastWeek!=rs.getInt("week")) {
                    lastWeek=rs.getInt("week");
                    sessionsShortList = filterRows(sessionsShortList);
                    sessionsList.addAll(sessionsShortList);
                    sessionsShortList.clear();
                    System.out.println("week: "+lastWeek);
                }
                if (currSessionId==-1) {
                    currSessionId = rs.getInt("sessionId");
                    sessions++;
                    sessionsShortList.add(new LogicalSession(true, currSessionId, currUser, new ArrayList<>()));
                    milisecs=currStamp-1000;
                } else if (currSessionId == rs.getInt("sessionId")) {
                    if (currStamp - milisecs > sessionTreshold) {
                        sessions++;
                        sessionsShortList.add(new LogicalSession(true, currSessionId, currUser, new ArrayList<>()));
                    }
                } else {
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
        sessionsShortList = filterRows(sessionsShortList);
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
        String connUrl="jdbc:sqlserver://mipdbserver;databaseName="+db_name;
        return DriverManager.getConnection(connUrl, user, password);
    }

}
