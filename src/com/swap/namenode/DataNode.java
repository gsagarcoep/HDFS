import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Set;

import com.google.protobuf.InvalidProtocolBufferException;

public class DataNode implements IDataNode {

	static IRequestParser requestParser = new ProtoRequestParser();
	static IResponseGenerator responseGenerator = new ProtoResponseGenerator();
	final int DN_RW_PASS=23;
	final int DN_RW_FAIL=29;

	static String rootPath = "/tmp"; // Get this from config file
	static String blocksListFile = "/tmp/blocksList" ;
	static Set<Integer> blocks;
	
	static int id = 1;
	static int port = 60010;
	static String ip = "0.0.0.0";
	
	static int namenode_port = 60010;
	static String namenode_ip = "0.0.0.0";
	static int blocksize = 0;
	static int heartbeat_seconds = 0;
	static String hdfs_dir = "";
	static int blockreport_seconds = 0;
	
	static INameNode namenode_stub;
	static Registry registry;
	
	@Override
	public byte[] readBlock(byte[] readBlockRequest) {
		// TODO Auto-generated method stub
		
		ReadBlockRequest request = requestParser.readBlock(readBlockRequest);
		
		int blockNumber = request.blockNumber;
		
		ReadBlockResponse response = new ReadBlockResponse();
		
		try
		{
			byte [] data = Files.readAllBytes(Paths.get(rootPath + "/" + Integer.toString(blockNumber)));
		
			response.status = 0;
			response.data = data;
		}
		catch(Exception e)
		{
			response.status = -1;
			response.data = null;
		}
		
		return responseGenerator.readBlock(response);
	}

	@Override
	public byte[] writeBlock(byte[] writeBockRequest) {
		
		WriteBlockRequest request = requestParser.writeBlock(writeBockRequest);
		
		BlockLocations blockLocation = request.blockLocation;
		byte [] data = request.data;
		
		int blockNumber = blockLocation.blockNumber;
		
		List<DataNodeLocation> dataNodeLists = blockLocation.locations;
		
		for(DataNodeLocation dataNode : dataNodeLists)
		{
			// Call writeData method of this data node
		}
		
		WriteBlockResponse response = new WriteBlockResponse();
		response.status = 0;
		
		
		// Add this block to the list and also write to the file
		
		blocks.add(blockNumber);
		
		try
		{
			BufferedWriter writer = new BufferedWriter(new FileWriter(blocksListFile, true));
			writer.append("," + blockNumber);
			writer.flush();
			writer.close();
		}
		catch(Exception e)
		{
			
		}
		
		return responseGenerator.writeBlock(response);
	}
	
	public void writeData(int blockNumber, byte[] data)
	{
		try
		{
			FileOutputStream out = new FileOutputStream(rootPath+"/"+blockNumber);
			out.write(data);
		}
		catch(Exception e)
		{
			
		}
	}
	
	public void Initialize() throws IOException
	{
		// Read file and fill blocks list.
		
		BufferedReader reader = new BufferedReader(new FileReader(blocksListFile));
		
		String line = reader.readLine();
		
		for(String block : line.split(","))
		{
			try
			{
				blocks.add(Integer.parseInt(block));
			}
			catch(Exception e)
			{
				
			}
		}
		
	}
	
	@Override
	public void sendHeartBeat() {
		
		HDFS.HeartBeatResponse hbResponse;
		while(true){
			HDFS.HeartBeatRequest.Builder hbRequest = HDFS.HeartBeatRequest.newBuilder();
			hbRequest.setId(id);
			try {
				hbResponse = HDFS.HeartBeatResponse.parseFrom(namenode_stub.heartBeat(hbRequest.build().toByteArray()));
				System.out.println("Sending Heartbeat");
	
				if (hbResponse.getStatus() == 0)
					System.out.println("Heartbeat status received : PASS");
				else if (hbResponse.getStatus() != 0)
					System.out.println("Heartbeat status received : FAIL");
				Thread.sleep(this.heartbeat_seconds*1000);
			
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void sendBlockReport()
	{
		// Read blocksListFile file and send report.
		while(true){
			HDFS.BlockReportResponse brResponse;
			HDFS.BlockReportRequest.Builder brRequest = HDFS.BlockReportRequest.newBuilder();
			brRequest.setId(this.id);
			
			HDFS.DataNodeLocation.Builder dnl = HDFS.DataNodeLocation.newBuilder();
			dnl.setIp(this.ip);
			dnl.setPort(this.port);
			brRequest.setLocation(dnl);
			
			// read all file names written under hdfs folder
			File folder=new File(this.hdfs_dir);
			File[] files=folder.listFiles();
			int i=0;
			for(File file: files) {
				brRequest.addBlockNumbers(Integer.parseInt(file.getName()));
				i++;
			}
			System.out.println("Sending block report");
			try {
				brResponse=HDFS.BlockReportResponse.parseFrom(namenode_stub.blockReport(brRequest.build().toByteArray()));
			
				System.out.println("Block report status received : \n Block Number \t Status\n");
				for(i=0;i< brResponse.getStatusCount(); i++)
					System.out.println(brRequest.getBlockNumbers(i)+" \t "+((brResponse.getStatus(i) == 0)? "PASS":"FAIL"));
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
		}
	
	}
	
	public static void main(String [] args) throws RemoteException
	{
		if(args.length < 1 )
		{
			System.out.println("Enter the config file anem with path");
			return;
		}	
		String configFile = args[0];
		
		//Set RMI Security manager
		if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
		//Get the data from config file
		DataNode currentDataNode = new DataNode();
		try {
			BufferedReader br=new BufferedReader(new FileReader(configFile));
			
			currentDataNode.id = Integer.parseInt(br.readLine().split(" ")[1].trim());
			currentDataNode.ip = br.readLine().split(" ")[1].trim();
			currentDataNode.port = Integer.parseInt(br.readLine().split(" ")[1].trim());
			currentDataNode.blocksize = Integer.parseInt(br.readLine().split(" ")[1].trim());
			currentDataNode.namenode_ip = br.readLine().split(" ")[1].trim();
			currentDataNode.namenode_port = Integer.parseInt(br.readLine().split(" ")[1].trim());
			currentDataNode.hdfs_dir = br.readLine().split(" ")[1].trim();
			currentDataNode.heartbeat_seconds = Integer.parseInt(br.readLine().split(" ")[1].trim());
			currentDataNode.blockreport_seconds = Integer.parseInt(br.readLine().split(" ")[1].trim());
			
			currentDataNode.registry = LocateRegistry.getRegistry(currentDataNode.namenode_ip);
			currentDataNode.namenode_stub = (INameNode) registry.lookup("namenode");
			
			System.out.println("***************** NameNode bound *******************");
			br.close();
			
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	

		IDataNode datanode_stub = (IDataNode) UnicastRemoteObject.exportObject((Remote) currentDataNode, 0);
		
		Registry registry = LocateRegistry.getRegistry(port);
		registry.rebind("datanode", (Remote) datanode_stub);
        System.out.println("***************** DataNode bound *******************");
        
		// Create a thread which will go in a loop to send heart beats. Also need to send block report ? same functions??
		HeartBeatRunnable hbrun =new HeartBeatRunnable(currentDataNode);
		Thread currentThread = new Thread(hbrun);
		currentThread.start();
		
		// Start accepting requests for read and write.
		currentDataNode.sendBlockReport();
		
		
		return;
	}

	

}
