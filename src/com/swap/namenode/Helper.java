import java.util.List;

public class Helper {

}

class DataNodeLocation
{
	String ip;
	int port;
}

enum mode
{
	Read,
	Write
}

class OpenFileRequest
{
	String fileName;
	Boolean forRead;
}

class CloseFileRequest
{
	int handle;
	HDFS.CloseFileRequest closeFileRequest;
}


class AssignBlockRequest
{
	int handle;
}


class ListRequest
{
	String dirName;
}

class BlockReportRequest
{
	int id;
	DataNodeLocation location;
	List<Integer> blockNumbers;
}

class HeartBeatRequest
{
	int id;
}

class ReadBlockRequest
{
	int blockNumber;
}

class WriteBlockRequest
{
	BlockLocations blockLocation;
	byte[] data;
}

class OpenFileResponse
{
	int status;
	int handle;
	List<Integer> blockNums;
}

class CloseFileResponse
{
	int status;
}

class BlockLocationRequest
{
	List<Integer> blockNums;
}

class BlockLocations
{
	int blockNumber;
	List<DataNodeLocation> locations;
}

class BlockLocationResponse
{
	int status;
	List<BlockLocations> blockLocations;
}

class AssignBlockResponse
{
	int status;
	BlockLocations blockLocations;
}


class ListResponse
{
	int status;
	List<String> fileNames;
}

class BlockReportResponse
{
	List<Integer> status;
}

class HeartBeatResponse
{
	int status;
}

class WriteBlockResponse
{
	int status;
}

class ReadBlockResponse
{
	int status;
	byte [] data;
}

class OpenFileDetails
{
	String name;
	int refCount;
	
	public OpenFileDetails(String name, int refCount)
	{
		this.name = name;
		this.refCount = refCount;
	}
}

class HeartBeatRunnable implements Runnable
{
    private DataNode dataNode;
    public HeartBeatRunnable(DataNode dn)
    {
        this.dataNode = dn;
    }
    public void run()
    {
    	dataNode.sendHeartBeat();
    }
}