package tw.idv.ctfan.cloud.middleware.policy.data;

public class ClusterNode implements Comparable<ClusterNode>
{
	// Agent Related Information
	public String name;
	public String address;
	public String container;
	public int    load;
	
	// System Related Information
	public long	  core;
	public long   memory;
	public long   CPURate;
	public String vmUUID;
	//public String siteIP;
	public VMMasterNode vmMaster;
		
	// Hadoop Related Information
	public int maxMapSlot;
	public int maxReduceSlot;
	
	
	public ClusterNode(String name, String container, String address)
	{
		this.name = name;
		this.container = container;
		this.address = address;
		this.load = 0;
		this.vmUUID = null;
	}
	
	public ClusterNode(VMMasterNode vmMaster, String vmUUID, String name, long core, long memory, long CPURate) {
		this.name = name;
		this.container = "N//A";
		this.address = "N//A";
		this.core = core;
		this.CPURate = CPURate;
		this.memory = memory;
		this.vmUUID = vmUUID;
		this.vmMaster = vmMaster;
		if(this.vmMaster == null)
			System.out.println(name + " has a null vmMaster");
	}
	
	public ClusterNode (int maxMapSlot, int maxReduceSlot)
	{
		this.maxMapSlot = maxMapSlot;
		this.maxReduceSlot = maxReduceSlot;
	}
	
	public ClusterNode(String s)
	{
		this.name = "N/A";
		this.container = "N/A";
		this.address = "N/A";
		this.load = -1;
		
		this.parseString(s);
	}
	
	public boolean compare(String name, String container, String address)
	{
		if( this.name.compareTo(name)==0 &&
			this.container.compareTo(container)==0 &&
			this.address.compareTo(address)==0)
			return true;
		else
			return false;
			
	}
	
	public static String toHTMLHead()
	{
		String result =
			"<tr>" +
				"<th>" + "Name" + "</th>" +
				"<th>" + "Container" + "</th>" +
				"<th>" + "Address" + "</th>" +
				"<th>" + "Map Task Capacity" + "</th>" +
				"<th>" + "Reuce Task Capacity" + "</th>" +
				"<th>" + "Load" + "</th>" +
			"</tr>";
		
		return result;
	}
	
	public String toHTML()
	{
		String result =
			"<tr>" +
				"<td>" + this.name + "</td>" +
				"<td>" + this.container + "</td>" +
				"<td>" + this.address + "</td>" +
				"<td>" + this.maxMapSlot + "</td>" +
				"<td>" + this.maxReduceSlot + "</td>" +
				"<td>" + this.load + "</td>" +
			"</tr>";
		
		return result;
	}
	
	public boolean compare(ClusterNode cn)
	{
		return this.compare(cn.name, cn.container, cn.address);
	}
	
	public String toString()
	{
		return name + " " + container + " " + address + " " + load;
	}
	
	public void parseString(String s)
	{
		String[] subInfo = s.split(" ");
		if(subInfo.length != 4)
		{
			System.err.println("Cluster Node parse Error");
			System.err.println("\t" + s);
			return;
		}
		
		name = subInfo[0];
		container = subInfo[1];
		address = subInfo[2];
		load = Integer.parseInt(subInfo[3]);
	}


	@Override
	public int compareTo(ClusterNode o) {
//		System.out.println(this.name + " Compare " + o.name);
		return this.name.compareTo(o.name);
	}
}