package tw.idv.ctfan.cloud.Middleware.policy.Decision;

import tw.idv.ctfan.cloud.Middleware.policy.data.ClusterNode;

public class VMManagementDecision {
	
	public ClusterNode cluster;
	public Command command;
	
	public static enum Command{
		START_VM,CLOSE_VM
	}
	
	
	public VMManagementDecision(ClusterNode cluster, Command command) {
		this.cluster = cluster;
		this.command = command;
	}

}
