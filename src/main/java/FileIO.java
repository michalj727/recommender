import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileIO {
    private String x;

    FileIO(String x) {
        this.x=x;

    }

    public void createFile(String xx, List<String> regSeq) {
        try {
            FileChannel rwChannel = new RandomAccessFile(x+"\\"+xx, "rw").getChannel();
            rwChannel.truncate(0);

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

    public void loadFile(int xx, ArrayList<String> regSeq) {
        Path y = FileSystems.getDefault().getPath(x+"\\docs"+xx+".txt");
        try (InputStream in = Files.newInputStream(y);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))
        ) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                regSeq.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
