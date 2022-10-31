import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class UniqueIds {
        private Connection connReport;
        private Connection connMip;

        public final Object2LongOpenHashMap<String> ids = new Object2LongOpenHashMap<String>();
        public final Long2ObjectOpenHashMap<String> ids1 = new Long2ObjectOpenHashMap<String>();
        public final Long2ObjectOpenHashMap<String> legalisIds = new Long2ObjectOpenHashMap<String>();
        public final Long2ObjectOpenHashMap<String> productIds = new Long2ObjectOpenHashMap<String>();
        public final Long2ObjectOpenHashMap<String> documentTypes = new Long2ObjectOpenHashMap<String>();
        public long lastCoor=-1;
        public long exportsInSeq=0;
        private long maxId=-1;
        private TJIndex tj;


        public UniqueIds(Connection connReport, Connection connMip) throws Exception {
            System.out.println("loading ids");
            this.connReport=connReport;
            this.connMip=connMip;
            tj = new TJIndex(connMip);
            PreparedStatement st = this.connReport.prepareStatement("select id1, id2, uniqId, legalisName, productName, documentType from booksids order by uniqId desc");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                if (maxId==-1) {
                    maxId=rs.getInt(3);
                }
                ids.put(rs.getInt(1)+":"+rs.getInt(2), rs.getInt(3));
                ids1.put(rs.getInt(3), rs.getInt(1)+":"+rs.getInt(2));
                legalisIds.put(rs.getInt(3), rs.getString(4).trim());
                productIds.put(rs.getInt(3), rs.getString(5).trim());
                documentTypes.put(rs.getInt(3), rs.getString(6).trim());
            }
            System.out.println(ids.size()+" ids loaded");
        }

        public long addItem(long id1, long id2, String legalisName, String productName, String documentType) throws Exception {
            maxId++;
            connReport.prepareStatement("insert into booksids(id1, id2, legalisName, productName, uniqId, documentType) values("+id1+","+id2+",'"+legalisName+"','"+productName+"',"+maxId+",'"+documentType.trim()+"')").executeUpdate();
            ids.put(id1+":"+id2, maxId);
            ids1.put(maxId, id1+":"+id2);
            legalisIds.put(maxId, legalisName.trim());
            productIds.put(maxId, productName.trim());
            documentTypes.put(maxId, documentType.trim());
            return maxId;
        }

        public long get(String docId, String prodId, String documentType) throws Exception {
            try {
                long id1=0, id2=0;
                String[] x = docId.split("\\.");
                if (x.length>1) {
                    if (x[1].contains("_")) {
                        id1 = Long.parseLong(x[1].substring(0, x[1].indexOf("_")));
                    } else {
                        id1 = Long.parseLong(x[1]);
                    }
                    if (x.length>2) {
                        if (x[2].equals("PAP")) {
                            id2 = Long.parseLong(x[3]);
                            id2 = tj.getAncestorOrSelf(id2);
                        } else if (x[2].equals("ACT") && x.length>4) {
                            id2 = Long.parseLong(x[5]);
                            id2 = tj.getAncestorOrSelf(id2);
                        }
                    }
                } else {
                    System.out.println("brak BOOK: "+docId);
                }

                String xx = id1+":"+id2;
                if (!ids.containsKey(xx)) {
                    addItem(id1, id2, docId, prodId, documentType);
                }
                return ids.getLong(xx);
            } catch (Exception e) {
                throw new Exception("ids get: "+docId+":"+e.getMessage());
            }
        }

        public String get1(int id) throws Exception {
            return ids1.get(id);
        }

        public long getBookid(long uniqId) {
                String x = ids1.get(uniqId);
                return Long.parseLong(x.substring(0, x.indexOf(":")));
        }

    public String getProductId(long uniqId) {
        return productIds.get(uniqId);
    }


    public long getPapid(long uniqId) {
            String x = ids1.get(uniqId);
            return Long.parseLong(x.substring(x.indexOf(":")+1));
        }

}
