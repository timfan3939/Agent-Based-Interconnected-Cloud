package tw.idv.ctfan.cloud.middleware.policy.data;

import java.net.URL;

import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Session;
import com.xensource.xenapi.VM;
import com.xensource.xenapi.Types;


public class VMMasterNode {
	
	public String IP;
	public String account;
	public String password;
	
	public Connection xenConnection;
	
	public static final int PRIVATE = 0x201;
	public static final int PUBLIC  = 0x202;
	public int masterType;
	
	public VMMasterNode(String IP, String account, String password, int masterType) {
		this.IP = IP;
		this.account = account;
		this.password = password;
		this.masterType = masterType;
		
		try {
			xenConnection = new Connection(new URL("http://" + IP));
			Session.loginWithPassword(xenConnection, account, password, null);
				
		} catch(Exception e) {
			System.err.println("Problem while connection to server " + IP);
			System.err.println("Acc: " + account + " pw: " + password);
		}
	}

}
