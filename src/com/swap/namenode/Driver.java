import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class Driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		HDFS.ReadBlockRequest.Builder op = HDFS.ReadBlockRequest.newBuilder();
		op.setBlockNumber(7);
		op.build();
		
		HDFS.BlockLocationRequest.Builder op1 = HDFS.BlockLocationRequest.newBuilder();
		op1.addBlockNums(1);
		op1.addBlockNums(2);
		op1.build();
		
		//byte[] readBlockRequest = ByteBuffer.allocate(4).putInt(7).array();
		ProtoRequestParser driver= new ProtoRequestParser();
		System.out.println(driver.readBlock(op.build().toByteArray()).blockNumber);
		System.out.println(driver.getBlockLocations(op1.build().toByteArray()).blockNums);
		
		ProtoResponseGenerator passenger = new ProtoResponseGenerator();
		OpenFileResponse a = new OpenFileResponse();
		a.status=1;
		a.handle=1;
		//System.out.println("12  ");
		a.blockNums= new ArrayList<Integer>();
		a.blockNums.add(12);
		a.blockNums.add(13);
		a.blockNums.add(22222);
		/*a.blockNums.set(2,13);
		*/
		//System.out.println("12  ");
	//	System.out.println("12  "+passenger.openFile(a));
	
	
		ProtoResponseParser passenger2 = new ProtoResponseParser();
		System.out.println(passenger2.openFile(passenger.openFile(a)));
	
	}
	
	

}
