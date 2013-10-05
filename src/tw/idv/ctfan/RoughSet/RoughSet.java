package tw.idv.ctfan.RoughSet;

import java.util.ArrayList;

public class RoughSet {
//	private static boolean debug = true;
	
	private final int m_numOfCondAttr;
	
	public class Element {
		public long[] attribute;
		public long decision;
		public QuineMcCluskey decisionRule;
		
		public Element(long[] attributeValue, long decisionValue) {
			attribute = attributeValue.clone();
			decision = decisionValue;
		}
		
		public boolean Similar(Element cmp) {
			if(decisionRule == null) {
				decisionRule = new QuineMcCluskey(m_leastReductSize);
				
				for(Element e:m_elements) {
					if(e==this) continue;
					if(e.decision==this.decision) continue;
					
					boolean[] diff = new boolean[m_leastReductSize];
					for(int i=0, count=0; i<attribute.length; i++) {
						if(m_leastReductAttr[i]) {
							diff[count] = (e.attribute[i]!=this.attribute[i]);
							count++;
						}
					}
					decisionRule.AddDNF(diff);
				}
				decisionRule.Calculate();
//				decisionRule.ShowReductAttribute();
//				decisionRule.ShowSmallestReductAttribute(m_leastCoreAttr);
//				System.out.println("================================================");
			}
			boolean[] decision = decisionRule.GetSmallestReductAttribute(m_reducedCoreAttr);
			
			for(int a=0, offset=0; a<m_numOfCondAttr; a++) {
				if(m_leastReductAttr[a]) {
					if(decision[offset]) {
						if(this.attribute[a]!=cmp.attribute[a])
							return false;
					}
					offset++;
				}
			}			
			
			return true;
		}
	}
	
	private ArrayList<Element> m_elements;
	
	public RoughSet(int numberOfConditionAttribute) {
		this.m_numOfCondAttr = numberOfConditionAttribute;
		m_elements = new ArrayList<Element>();
	}
	
	public boolean AddElement(Element element) {
		if(element.attribute.length!=m_numOfCondAttr) {
			return true;
		}
		m_elements.add(element);
		m_calculated = false;
		return false;
	}
	
	private boolean m_calculated = false;
	
	QuineMcCluskey m_mainTabular;
	boolean[] m_leastReductAttr;
	boolean[] m_coreAttr;
	boolean[] m_reducedCoreAttr;
	int m_leastReductSize;
	
	public void Calculate() {
		if(m_calculated) return;
		
		m_mainTabular = new QuineMcCluskey(m_numOfCondAttr);
		ComputeDiscernibilityMatrix(m_mainTabular);
		m_mainTabular.Calculate();
		
		m_leastReductAttr = m_mainTabular.GetSmallestReductAttribute();
		m_leastReductSize = NumOfTrue(m_leastReductAttr);
		m_coreAttr = m_mainTabular.GetCoreAttribute();
		
		m_reducedCoreAttr = new boolean[m_leastReductSize];
		for(int i=0, offset = 0; i<m_coreAttr.length; i++) {
			if(m_leastReductAttr[i]){
				m_reducedCoreAttr[offset] = m_coreAttr[i];
				offset++;
			}
		}
		
		m_calculated = true;
	}
	
	private static int NumOfTrue(boolean[] array) {
		int count = 0;
		for(boolean b:array){
			if(b) count++;
		}
		
		return count;
	}
		
	private void ComputeDiscernibilityMatrix(QuineMcCluskey tabular){
		for(int i=0; i<m_elements.size(); i++) {
			Element e1 = m_elements.get(i);
			e1.decisionRule = null;
			for(int j=i+1; j<m_elements.size(); j++) {
				Element e2 = m_elements.get(j);
				if(e1.decision == e2.decision) {
					continue;
				}
				else {
					boolean[] diff = new boolean[m_numOfCondAttr];
					for(int k=0; k<m_numOfCondAttr; k++) {
						if(e1.attribute[k]==e2.attribute[k]) {
							diff[k] = false;
						} else {
							diff[k] = true;
						}
					}
					tabular.AddDNF(diff);
				}
			}
		}
	}	
	
	public boolean[][] GetReductAttribute(){
		return m_mainTabular.GetReductAttribute();
	}

	public boolean[] GetCoreAttribute() {
		return m_mainTabular.GetCoreAttribute();
	}
	
	public void ShowReductAttribute() {
		m_mainTabular.ShowReductAttribute();
	}
	
	public void ShowCoreAttribute() {
		m_mainTabular.ShowCoreAttribute();
	}
	
	public long GetDecision(Element decision){
		int i=0;
		for(Element e:m_elements) {
//			System.out.println("Testing " + i++ + " Object");
			if(e.Similar(decision))
				System.out.println("Match Object " + i);
			i++;
		}
		
		return 0;
	}
}

