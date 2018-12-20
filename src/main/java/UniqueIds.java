import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class UniqueIds {
        private Connection connReport;
        public final Object2IntOpenHashMap<String> ids = new Object2IntOpenHashMap<String>();
        public final Int2ObjectOpenHashMap<String> ids1 = new Int2ObjectOpenHashMap<String>();
        public final Int2ObjectOpenHashMap<String> legalisIds = new Int2ObjectOpenHashMap<String>();
        public int lastCoor=-1;
        public int exportsInSeq=0;
        private int maxId=-1;


        public UniqueIds(Connection connReport) throws Exception {
            System.out.println("loading ids");
            this.connReport=connReport;
            PreparedStatement st = this.connReport.prepareStatement("select id1, id2, uniqId, legalisName from booksids order by uniqId desc");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                if (maxId==-1) {
                    maxId=rs.getInt(3);
                }
                ids.put(rs.getInt(1)+":"+rs.getInt(2), rs.getInt(3));
                ids1.put(rs.getInt(3), rs.getInt(1)+":"+rs.getInt(2));
                legalisIds.put(rs.getInt(3), rs.getString(4).trim());
            }
            System.out.println(ids.size()+" ids loaded");
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
            try {
                String xx = id1+":"+id2;
                if (!ids.containsKey(xx)) {
                    addItem(id1, id2, legalisName.trim());
                }
                return ids.getInt(xx);
            } catch (Exception e) {
                throw new Exception("ids get: "+id1+":"+id2+":"+legalisName+":"+e.getMessage());
            }
        }

        public String get1(int id) throws Exception {
            return ids1.get(id);
        }
}
