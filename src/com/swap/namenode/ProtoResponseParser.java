import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class ProtoResponseParser implements IResponseParser {

	@Override
	public OpenFileResponse openFile(byte[] openFileResponse) {
		// TODO Auto-generated method stub
		OpenFileResponse curr = new OpenFileResponse();
		
		InputStream in = new ByteArrayInputStream(openFileResponse);
		try {
			HDFS.OpenFileResponse input = HDFS.OpenFileResponse.parseFrom(in);
			curr.handle = input.getHandle();
			curr.status = input.getStatus();
			
			curr.blockNums = input.getBlockNumsList();
		//	System.out.println(curr.blockNums);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public CloseFileResponse closeFile(byte[] closeFileResponse) {
		// TODO Auto-generated method stub
		CloseFileResponse curr = new CloseFileResponse();
		
		InputStream in = new ByteArrayInputStream(closeFileResponse);
		try {
			HDFS.CloseFileResponse input = HDFS.CloseFileResponse.parseFrom(in);
			curr.status = input.getStatus();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public BlockLocationResponse getBlockLocations(
			byte[] getBlockLocationsResponse) {
		// TODO Auto-generated method stub
		BlockLocationResponse curr = new BlockLocationResponse();
		
		InputStream in = new ByteArrayInputStream(getBlockLocationsResponse);
		try {
			HDFS.BlockLocationResponse input = HDFS.BlockLocationResponse.parseFrom(in);
			curr.status = input.getStatus();
			List<HDFS.BlockLocations> arr =  input.getBlockLocationsList();
			
			for(HDFS.BlockLocations temp : arr ){
				BlockLocations currentBlock = new BlockLocations();
				currentBlock.blockNumber = temp.getBlockNumber();
				
				List <HDFS.DataNodeLocation> brr = temp.getLocationsList();
				
				for(HDFS.DataNodeLocation yy:brr){
					DataNodeLocation currentDatanode = new DataNodeLocation();
					currentDatanode.ip = yy.getIp();
					currentDatanode.port = yy.getPort();
					currentBlock.locations.add(currentDatanode);
				}
				curr.blockLocations.add(currentBlock);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public AssignBlockResponse assignBlock(byte[] assignBlockResponse) {
		// TODO Auto-generated method stub
		AssignBlockResponse curr = new AssignBlockResponse();
		
		InputStream in = new ByteArrayInputStream(assignBlockResponse);
		try {
			HDFS.AssignBlockResponse input = HDFS.AssignBlockResponse.parseFrom(in);
			curr.status = input.getStatus();
			HDFS.BlockLocations temp = input.getNewBlock();
			
			List<HDFS.DataNodeLocation> arr = temp.getLocationsList();
			
			for(HDFS.DataNodeLocation here: arr){
				
				DataNodeLocation cur = new DataNodeLocation();
				cur.ip = here.getIp();
				cur.port = here.getPort();
				curr.blockLocations.locations.add(cur);
			}
		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public ListResponse list(byte[] listResponse) {
		// TODO Auto-generated method stub
		ListResponse curr = new ListResponse();
		
		InputStream in = new ByteArrayInputStream(listResponse);
		try {
			HDFS.ListFilesResponse input = HDFS.ListFilesResponse.parseFrom(in);
			curr.status = input.getStatus();
			List<String > arr = input.getFileNamesList();
			
			for(String name:arr){
				curr.fileNames.add(name);
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public BlockReportResponse blockReport(byte[] blockReportResponse) {
		// TODO Auto-generated method stub
		BlockReportResponse curr = new BlockReportResponse();
		
		InputStream in = new ByteArrayInputStream(blockReportResponse);
		try {
			HDFS.BlockReportResponse input = HDFS.BlockReportResponse.parseFrom(in);
			curr.status = input.getStatusList();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public HeartBeatResponse heartBeat(byte[] heartBeatResponse) {
		// TODO Auto-generated method stub
		HeartBeatResponse curr = new HeartBeatResponse();
		
		InputStream in = new ByteArrayInputStream(heartBeatResponse);
		try {
			HDFS.HeartBeatResponse input = HDFS.HeartBeatResponse.parseFrom(in);
			curr.status = input.getStatus();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public ReadBlockResponse readBlock(byte[] readBlockResponse) {
		// TODO Auto-generated method stub
		ReadBlockResponse curr = new ReadBlockResponse();
		
		InputStream in = new ByteArrayInputStream(readBlockResponse);
		try {
			HDFS.ReadBlockResponse input = HDFS.ReadBlockResponse.parseFrom(in);
			curr.status = input.getStatus();
			curr.data = input.getData(0).toByteArray();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public WriteBlockResponse writeBlock(byte[] writeBlockResponse) {
		// TODO Auto-generated method stub
		WriteBlockResponse curr = new WriteBlockResponse();
		
		InputStream in = new ByteArrayInputStream(writeBlockResponse);
		try {
			HDFS.WriteBlockResponse input = HDFS.WriteBlockResponse.parseFrom(in);
			curr.status = input.getStatus();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

}
