package tw.idv.ctfan.cloud.middleware.test;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;

public class testRoughSet {
	
	public static void main(String[] args) {
		
		ExecutionTimePolicy p = (ExecutionTimePolicy) ExecutionTimePolicy.GetPolicy();
		
		ArrayList<JobNodeBase> list = p.GetFinishJob();
		
		list.add(new HadoopJobNode(4,4,11455464,20,408000,73784,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,192731,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,150373,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,164062,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,207616,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,243471,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,90401,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,189321,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,36066,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,70687,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,48162,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,129289,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,181740,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,168411,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,135270,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114165,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,112655,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,99624,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,78616,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,135221,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,74300,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,192797,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,150397,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,198225,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,213415,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,249469,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,186413,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,48084,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,90511,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,57123,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,45071,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,135207,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,180387,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,174415,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114192,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,159280,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,96211,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,111212,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114127,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,81813,0));
				
		long start = System.currentTimeMillis();
		p.RefreshRoughSet();
		System.out.println(p.GetDecision(new HadoopJobNode(4,4,11455464,20,408000,0,0)));
		System.out.println(p.GetDecision(new HadoopJobNode(4,4,11455464,50,1020000,0,0)));
		System.out.println(p.GetDecision(new HadoopJobNode(4,4,11455464,100,2040000,0,0)));
		long end = System.currentTimeMillis();
		
		System.out.println(end - start);
		
	}

}
