import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;


public class ProtoRequestParser implements IRequestParser{

	@Override
	public ReadBlockRequest readBlock(byte[] readBlockRequest) {
		// TODO Auto-generated method stub
		
		ReadBlockRequest currentReadBlockRequest = new ReadBlockRequest();
		try {
			
			InputStream in = new ByteArrayInputStream(readBlockRequest);
			HDFS.ReadBlockRequest input = HDFS.ReadBlockRequest.parseFrom(in);
			int temp =0;
			temp = input.getBlockNumber();
			currentReadBlockRequest.blockNumber=temp;
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return currentReadBlockRequest;
	}

	@Override
	public WriteBlockRequest writeBlock(byte[] writeBlockRequest) {
		// TODO Auto-generated method stub
		WriteBlockRequest currentWriteBlockRequest = new WriteBlockRequest();
		
		InputStream in = new ByteArrayInputStream(writeBlockRequest);
		try {
			HDFS.WriteBlockRequest temp = HDFS.WriteBlockRequest.parseFrom(in);
			
			currentWriteBlockRequest.blockLocation.blockNumber = temp.getBlockInfo().getBlockNumber();
			
			List<HDFS.DataNodeLocation> arr = temp.getBlockInfo().getLocationsList();
			
			for(HDFS.DataNodeLocation here: arr){
				
				DataNodeLocation cur = new DataNodeLocation();
				cur.ip = here.getIp();
				cur.port = here.getPort();
				currentWriteBlockRequest.blockLocation.locations.add(cur);
			}
			//Only first element in the bytestring list will be filled.
			currentWriteBlockRequest.data = temp.getData(0).toByteArray();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return currentWriteBlockRequest;
	}

	@Override
	public OpenFileRequest openFile(byte[] openFileRequest) {
		// TODO Auto-generated method stub
		OpenFileRequest curr = new OpenFileRequest();
		
		InputStream in = new ByteArrayInputStream(openFileRequest);
		try {
			HDFS.OpenFileRequest input = HDFS.OpenFileRequest.parseFrom(in);
			curr.fileName = input.getFileName();
			curr.forRead = input.getForRead();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return curr;
	}

	@Override
	public CloseFileRequest closeFile(byte[] closeFileRequest) {
		// TODO Auto-generated method stub
	
		CloseFileRequest curr = new CloseFileRequest();
		
		InputStream in = new ByteArrayInputStream(closeFileRequest);
		try {
			curr.closeFileRequest = HDFS.CloseFileRequest.parseFrom(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

	@Override
	public BlockLocationRequest getBlockLocations(byte[] getBlockLocationsRequest) {
		// TODO Auto-generated method stub
		BlockLocationRequest curr = new BlockLocationRequest();
		
		InputStream in = new ByteArrayInputStream(getBlockLocationsRequest);
		try {
			HDFS.BlockLocationRequest input = HDFS.BlockLocationRequest.parseFrom(in);
			curr.blockNums = input.getBlockNumsList();
			//System.out.println(curr.blockNums);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

	@Override
	public AssignBlockRequest assignBlock(byte[] assignBlockRequest) {
		// TODO Auto-generated method stub
		AssignBlockRequest curr = new AssignBlockRequest();
		
		InputStream in = new ByteArrayInputStream(assignBlockRequest);
		try {
			HDFS.AssignBlockRequest temp = HDFS.AssignBlockRequest.parseFrom(in);
			curr.handle = temp.getHandle();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

	@Override
	public ListRequest list(byte[] listRequest) {
		// TODO Auto-generated method stub
		ListRequest curr = new ListRequest();
		
		InputStream in = new ByteArrayInputStream(listRequest);
		try {
		
			curr.dirName = HDFS.ListFilesRequest.parseFrom(in).getDirName();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

	@Override
	public BlockReportRequest blockReport(byte[] blockReportRequest) {
		// TODO Auto-generated method stub
		BlockReportRequest curr = new BlockReportRequest();
		
		InputStream in = new ByteArrayInputStream(blockReportRequest);
		try {
			HDFS.BlockReportRequest temp = HDFS.BlockReportRequest.parseFrom(in);
			curr.blockNumbers = temp.getBlockNumbersList();
			curr.id = temp.getId();
			
			DataNodeLocation dnl = new DataNodeLocation();
			dnl.ip = temp.getLocation().getIp();
			dnl.port = temp.getLocation().getPort();
			curr.location = dnl;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

	@Override
	public HeartBeatRequest heartBeat(byte[] heartBeatRequest) {
		// TODO Auto-generated method stub
		HeartBeatRequest curr = new HeartBeatRequest();
		
		InputStream in = new ByteArrayInputStream(heartBeatRequest);
		try {
			curr.id = HDFS.HeartBeatRequest.parseFrom(in).getId();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return curr;
	}

}
