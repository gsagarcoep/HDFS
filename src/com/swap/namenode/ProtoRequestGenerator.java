import com.google.protobuf.ByteString;

public class ProtoRequestGenerator implements IRequestGenerator
{

	@Override
	public byte[] openFile(OpenFileRequest openFileRequest) {
		// TODO Auto-generated method stub
		
		HDFS.OpenFileRequest.Builder op = HDFS.OpenFileRequest.newBuilder();
		op.setFileName(openFileRequest.fileName);
		op.setForRead(openFileRequest.forRead);
		
		return op.build().toByteArray();
	}

	@Override
	public byte[] closeFile(CloseFileRequest closeFileRequest) {
		// TODO Auto-generated method stub
		
		return closeFileRequest.closeFileRequest.toByteArray();
	}

	@Override
	public byte[] getBlockLocations(
			BlockLocationRequest getBlockLocationsRequest) {
		// TODO Auto-generated method stub
		HDFS.BlockLocationRequest.Builder op = HDFS.BlockLocationRequest.newBuilder();
		
		for(int temp : getBlockLocationsRequest.blockNums){
			op.addBlockNums(temp);
		}
		return op.build().toByteArray();		
	}

	@Override
	public byte[] assignBlock(AssignBlockRequest assignBlockRequest) {
		// TODO Auto-generated method stub
		HDFS.AssignBlockRequest.Builder op = HDFS.AssignBlockRequest.newBuilder();
		
		op.setHandle(assignBlockRequest.handle);
		
		
		return op.build().toByteArray();		
	}
	@Override
	public byte[] list(ListRequest listRequest) {
		// TODO Auto-generated method stub
		HDFS.ListFilesRequest.Builder op = HDFS.ListFilesRequest.newBuilder();
		
		op.setDirName(listRequest.dirName);
		
		
		return op.build().toByteArray();		
	}

	@Override
	public byte[] blockReport(BlockReportRequest blockReportRequest) {
		// TODO Auto-generated method stub
		HDFS.BlockReportRequest.Builder op = HDFS.BlockReportRequest.newBuilder();
		
		op.setId(blockReportRequest.id);
		
		HDFS.DataNodeLocation.Builder opdn = HDFS.DataNodeLocation.newBuilder();
		opdn.setIp(blockReportRequest.location.ip);
		opdn.setPort(blockReportRequest.location.port);
		
		op.setLocation(opdn.build());
		
		for(int i:blockReportRequest.blockNumbers){
			op.addBlockNumbers(i);
			
		}
		
		return op.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(HeartBeatRequest heartBeatRequest) {
		// TODO Auto-generated method stub
		HDFS.HeartBeatRequest.Builder op = HDFS.HeartBeatRequest.newBuilder();
		
		op.setId(heartBeatRequest.id);
				
		return op.build().toByteArray();
	}

	@Override
	public byte[] readBlock(ReadBlockRequest readBlockRequest) {
		// TODO Auto-generated method stub
		HDFS.ReadBlockRequest.Builder op = HDFS.ReadBlockRequest.newBuilder();
		op.setBlockNumber(readBlockRequest.blockNumber);
		return op.build().toByteArray();
	}

	@Override
	public byte[] writeBlock(WriteBlockRequest writeBlockRequest) {
		// TODO Auto-generated method stub
		HDFS.WriteBlockRequest.Builder op = HDFS.WriteBlockRequest.newBuilder();
		
		op.addData(ByteString.copyFrom(writeBlockRequest.data));
		
		HDFS.BlockLocations.Builder opbl = HDFS.BlockLocations.newBuilder();
		
		for(DataNodeLocation temp: writeBlockRequest.blockLocation.locations){
			HDFS.DataNodeLocation.Builder opdn = HDFS.DataNodeLocation.newBuilder();
			opdn.setIp(temp.ip);
			opdn.setPort(temp.port);
			
			opbl.addLocations(opdn.build());
			
		}
			opbl.setBlockNumber(writeBlockRequest.blockLocation.blockNumber);
			op.setBlockInfo(opbl.build());
		
		return op.build().toByteArray();
	}

	
}
