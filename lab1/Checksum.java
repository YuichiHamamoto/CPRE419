import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class Checksum {
	public static void main(String[]args) throws Exception{
		Configuration c = new Configuration();
		FileSystem fs = FileSystem.get(c);
		String name = "/lab1/bigdata";
		Path path = new Path(name);
		FSDataInputStream file = fs.open(path);
		
		byte buffer[] = new byte[1000];
		file.readFully(1000000000, buffer);
		int check = buffer[0];
		
		for(int i=1;i<buffer.length;i++) {
			check^=buffer[i];
		}
		
		System.out.println("Checksum: "+check);
		file.close();
		fs.close();
	}

}
