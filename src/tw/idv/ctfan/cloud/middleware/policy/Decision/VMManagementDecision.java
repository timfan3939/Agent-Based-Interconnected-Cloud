package tw.idv.ctfan.cloud.middleware.policy.Decision;

import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;

public class VMManagementDecision {
	
	public ClusterNode cluster;
	public int command;
	
	public static final int START_VM = 0x301;
	public static final int CLOSE_VM = 0x302;
	
	public VMManagementDecision(ClusterNode cluster, int command) {
		this.cluster = cluster;
		this.command = command;
	}

}
