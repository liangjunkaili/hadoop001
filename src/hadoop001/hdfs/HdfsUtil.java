package hadoop001.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HdfsUtil {
	FileSystem fs = null;
	String HDFSUri = "hdfs://192.168.150.200:8020";
	public static void main(String[] args) {
		try {
			HdfsUtil hdfs = new HdfsUtil();
			hdfs.init();
			hdfs.upload("E:\\cluster\\Hadoop\\download\\qinmi.log","/input/qinmi.log");
//			hdfs.listFiles();
//			hdfs.mkdir();
//			hdfs.rm();
//			hdfs.download();
//			DatanodeInfo[] datanodes = hdfs.getHDFSNodes();
//			BlockLocation[] blocklocations = hdfs.getFileBlockLocations("/1.txt");
//			hdfs.rename();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFSUri);
		fs = FileSystem.get(new URI(HDFSUri),conf,"hadoop");
	}
	/**
	 * 上传一个文件 
	 * @throws IOException
	 */
	public void upload(String input,String output) throws IOException{
		Path dst = new Path(HDFSUri+output);
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream(input);
		IOUtils.copy(is, os);
	}
	/**
	 * 上传文件2
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void upload2() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path(""), new Path(""));
	}
	/**
	 * 下载文件
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void download() throws IllegalArgumentException, IOException{
		
//		fs.copyToLocalFile(new Path("hdfs://192.168.150.200:8020/1.txt"),new Path("E:\\cluster\\Hadoop\\download\\2.txt"));
//		fs.close();
		FSDataInputStream is = fs.open(new Path(HDFSUri+"/1.txt"));
		FileOutputStream out = new FileOutputStream(new File("E:\\cluster\\Hadoop\\download\\3.txt"));
        //再将输入流中数据传输到输出流
        org.apache.hadoop.io.IOUtils.copyBytes(is, out, 4096);
	}
	/**
	 * 查看文件信息
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while(files.hasNext()){
			LocatedFileStatus file = files.next();
			Path filePath = file.getPath();
			String fileName = filePath.getName();
			System.out.println(fileName);
		}
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status:listStatus){
			String name = status.getPath().getName();
			System.out.println("-------"+name);
		}
	}
	/**
	 * 创建文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public void mkdir() throws IllegalArgumentException, IOException{
		fs.mkdirs(new Path("/aaa/bbb"));
	}
	/**
	 * 删除文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public void rm() throws IllegalArgumentException, IOException{
		fs.delete(new Path("/aaa"), true);
	}
	public void rename() throws IllegalArgumentException, IOException{
		fs.rename(new Path(HDFSUri+"/1.txt"), new Path(HDFSUri+"/2.txt"));
	}
	/**
	   * 获取 HDFS 集群节点信息
	   * 
	   * @return DatanodeInfo[]
	   */
	  public DatanodeInfo[] getHDFSNodes() {
	      // 获取所有节点
	      DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
	      
	     try {
	         // 获取分布式文件系统
	         DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	         dataNodeStats = hdfs.getDataNodeStats();
	     } catch (IOException e) {
	         
	     }
	     return dataNodeStats;
	 }
	  /**
	    * 查找某个文件在 HDFS集群的位置
	    * 
	    * @param filePath
	    * @return BlockLocation[]
	    */
	   public BlockLocation[] getFileBlockLocations(String filePath) {
	       // 文件路径
	       String hdfsUri = HDFSUri;
	      if(StringUtils.isNotBlank(hdfsUri)){
	          filePath = hdfsUri + filePath;
	      }
	      Path path = new Path(filePath);
	      // 文件块位置列表
	      BlockLocation[] blkLocations = new BlockLocation[0];
	      try {
	          // 获取文件目录 
	          FileStatus filestatus = fs.getFileStatus(path);
	          //获取文件块位置列表
	          blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
	      } catch (IOException e) {
	      }
	      return blkLocations;
	  }
}
