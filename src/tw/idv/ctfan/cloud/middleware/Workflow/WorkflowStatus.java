package tw.idv.ctfan.cloud.middleware.Workflow;

import java.util.ArrayList;

public class WorkflowStatus {
	static ArrayList<String> UUIDinfo = new ArrayList<String>();
	public WorkflowStatus() {
			
	}
	public static void setUUID(int taskID, String UUID){
		UUIDinfo.add(taskID,UUID);
	}
	public static void setnewUUID(int taskID, String UUID){
		UUIDinfo.set(taskID, UUID);
	}
	public static String getUUID(int taskID){
		return UUIDinfo.get(taskID);
	}
	
	public static int getLength(){
		return UUIDinfo.size();
	}
}
