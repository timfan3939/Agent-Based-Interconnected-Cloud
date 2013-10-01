package tw.idv.ctfan.RoughSet;

import java.util.ArrayList;
import java.util.List;

public class RoughSet {
	
	/**
	 * Element List, the condition attribute list of elements.
	 */
	private ArrayList<int[]> m_element;
	
	/**
	 * Decision List, the decision attribute list of elements.
	 */
	private ArrayList<Integer>   m_elementDecision;
	
	/**
	 * Decision Reduct (D-reduct) of the rough set data, may contain more than
	 * one core.
	 */
	private ArrayList<Term> m_coreAttribute;
	
	/**
	 * Decision making table of the rough set data.
	 */
	private ArrayList<Term> m_conditionList;
	
	/**
	 * Condition attribute size.
	 */
	private int m_attributeSize;
	
	/**
	 * Check if the rough set data needs to be recompute.
	 */
	private boolean m_elementModified;
	
	/**
	 * Constructor of RoughSet
	 * 
	 * @param attr_size			the number of condition attribute
	 */
	public RoughSet(int attr_size)
	{
		m_element = new ArrayList<int[]>();
		m_elementDecision = new ArrayList<Integer>();
		m_attributeSize = attr_size;
		m_coreAttribute = new ArrayList<Term>();
		m_elementModified = false;
		if(m_attributeSize<0) m_attributeSize = 0;
	}
	
	/**
	 * Add new element to the element list
	 * 
	 * @param element			the condtion attribute list of the element
	 * @param elementDecision	the decision attribute of the element
	 * @return					false if the condition attribute's number is wrong
	 */
	public boolean AddElement(int element[], int elementDecision)
	{
		if(element.length!=m_attributeSize)
		{
			System.err.println("You should expand the attribute size first");
			return false;
		}
		
		m_elementModified = true;
		m_element.add(element.clone());
		m_elementDecision.add(elementDecision);
		
		return true;
	}	
	
	public boolean AddElement(Integer[] element, int elementDecision)
	{
		if(element.length!=m_attributeSize)
		{
			System.err.println("You should expand the attribute size first");
			return false;
		}
		
		int[] e = new int[element.length];
		for(int i=0; i<e.length; i++)
			e[i] = element[i];
		
		m_elementModified = true;
		m_element.add(e);
		m_elementDecision.add(elementDecision);
		
		return true;
	}	
	
	/**
	 * Find the Decision Reduct (D-core) of the rough set
	 */
	public void FindCore()
	{
		if(m_element.size() <= 0)
		{
			System.err.println("No Element Found");
			return;
		}
		
		ArrayList<Term> discernibilityFunction = new ArrayList<Term>();
		for(int i=0; i< m_element.size(); i++){
			int[] leftElement = m_element.get(i);			
			
			for(int compare= i+1; compare< m_element.size(); compare++)	{
				if(m_elementDecision.get(i)==m_elementDecision.get(compare)){
					continue;
				}				
				
				int[] rightElement = m_element.get(compare);
				byte[]result = new byte[m_attributeSize];
				
				boolean haveDifference = false;
				for(int resultIndex=0; resultIndex<m_attributeSize; resultIndex++)	{
					if(rightElement[resultIndex]==leftElement[resultIndex])	{
						result[resultIndex] = Term.DontCare;
					}else{
						result[resultIndex] = (byte)1;
						haveDifference = true;
					}
				}
				
				if(haveDifference){
					Term term = new Term(result);
					
					if(!discernibilityFunction.contains(term))
						discernibilityFunction.add(term);
				}
			}
		}
		
		Formula formula = new Formula(discernibilityFunction);
		formula.reduceToPrimeImplicants();		
		formula.reducePrimeImplicantsToSubset();
		
		List<Term> midResult = formula.toTermArray();		
		m_coreAttribute = ComputeCoreAttribute(midResult);
		
		
		//System.out.println("midResult " + midResult);
		//System.out.println("m_coreAttribute " + m_coreAttribute);
		
		
	}
	
	/**
	 * Find Each element's decision table
	 */
	public void FindDecisionList()
	{
		if(m_elementModified)
			return;
		if(m_coreAttribute.size()<=0)
			return;
		
		m_conditionList = new ArrayList<Term>();
		
		byte[] coreReduct = m_coreAttribute.get(0).toByteArray();

		for(int element=0; element<m_element.size(); element++)	{
			int[] currentElement = m_element.get(element);
			List<Term> terms = new ArrayList<Term>();
			
			for(int compareTo=0; compareTo<m_element.size(); compareTo++){
				if(element == compareTo)
					continue;
				if(m_elementDecision.get(element) == m_elementDecision.get(compareTo))
					continue;
				
//				System.out.println("Element " + element + " compare to " + compareTo);
				
				int[] comparedElement = m_element.get(compareTo);
				byte result[] = new byte[m_attributeSize];
				
				for(int condition=0; condition<m_attributeSize; condition++){
					if(coreReduct[condition]==1) {
						if(currentElement[condition] != comparedElement[condition])
							result[condition] = 1;
					} else
						result[condition] = Term.DontCare;
				}
				
//				System.out.print(" result " + new Term(result));
				
				int r = 0;
				for(int i=0; i<result.length; i++)
					if(result[i]!=Term.DontCare)
						r += result[i];
				
//				System.out.println(" " + r);
				
				if(r!=0) {
					Term t = new Term(result);
					terms.add(t);
//					System.out.println("Terms is being added an item " + terms.size());
				}
			}
			
//			System.out.println("---------");
//			for(Term t:terms) {
//				System.out.println(t);
//			}
//			System.out.println("----------");
//			
//			if(terms.size()==0) System.out.println("%%%%%%%%%%%% term size is zero %%%%%%%%%%%%%%");
			if(terms.size()==0) {
				System.out.println("Something goes wrong at " + element);
				terms.add(m_coreAttribute.get(0));
			}


//			System.out.println("===== Start simplify fomula =====");
			Formula formula = new Formula(terms);
			formula.reduceToPrimeImplicants();
			formula.reducePrimeImplicantsToSubset();
//			System.out.println("===== Finish simplify fomula =====");
			
			List<Term> midResult = formula.toTermArray();
			List<Term> result = ComputeCoreAttribute(midResult);
			
//			for( Term t: result) {
//				System.out.println(t);
//			}
						
			//if(result.size()>0)
				m_conditionList.add(result.get(0));
			
			
		}		
		//System.out.println("m_conditionList " + m_conditionList);
		System.out.println("My Element List:\t" + m_element.size());
		System.out.println("My Decision List:\t" + m_conditionList.size());
	}
	
	/**
	 * Transform (A+B)C to (AC+BC)
	 * 
	 * @param terms		the (A+B)C form list
	 * @return			the result container of terms
	 */
	private ArrayList<Term> ComputeCoreAttribute(List<Term> terms)
	{
		if(terms.size()==0)
			return null;
		
		ArrayList<Term> result = new ArrayList<Term>();
		
		byte[] combination = new byte[m_attributeSize];
		for(int i=0; i<m_attributeSize; i++)
		{
			combination[i] = Term.DontCare;
		}
		Term term = terms.get(0);
		byte[] thisLevel = term.toByteArray();
		for(int i=0; i < m_attributeSize; i++)
		{
			if(thisLevel[i]==1)
			{
				combination[i] = (byte)1;
				ComputeCoreAttribute(terms, 1, combination, result);
				combination[i] = Term.DontCare;
			}
		}
		m_elementModified = false;
		
		return result;
	}
	
	/**
	 * Transform (A+B)C to (AC+BC).  You should call 
	 * {@link #ComputeCoreAttribute(List)} instead of this one}
	 * 
	 * @param terms		the (A+B)C form list
	 * @param level		the level of current list
	 * @param previous	the previous result
	 * @param result	the final result container
	 */
	private void ComputeCoreAttribute(List<Term> terms, int level, byte[] previous, ArrayList<Term> result)
	{
		if(level >= terms.size())
		{
			boolean hasValue = false;
			for(int i=0; i<previous.length; i++)
			{
				if(previous[i]!=Term.DontCare)
					hasValue = true;
			}
			if(hasValue)
			{
				Term newTerm = new Term(previous);
				if(!result.contains(newTerm))
				{
					result.add(newTerm);
				}
			}
			return;
		}
		
		byte[] combination = previous.clone();
		Term term = terms.get(level);
		byte[] thisLevel = term.toByteArray();
		for(int i=0; i<m_attributeSize; i++)
		{
			if(thisLevel[i]==1 && previous[i]==Term.DontCare)
			{
				combination[i] = (byte)1;
				ComputeCoreAttribute(terms, level+1, combination, result);
				combination[i] = Term.DontCare;	
			}
		}
	}
	
	/**
	 * Get the result with some condition
	 * 
	 * @param testCase		the test case condition attribute
	 * @return				null if wrong condidion attribute or not yet compute core
	 */
	public Object[] GetExactDecision(int[] testCase)
	{
		if(m_elementModified || testCase.length!= m_attributeSize)
		{
			System.err.println("Elements have been modified");
			return null;
		}
		if(m_conditionList.size()!=m_element.size())
		{
			System.err.println("test case input error, Rough Set: " + m_conditionList.size() + " element " + m_element.size());
			return null;
		}
		
		ArrayList<Integer> result = new ArrayList<Integer>();
		
		for(int element=0; element<m_element.size(); element++)
		{
			int[] originalElement = m_element.get(element);
			byte[] conditionList = m_conditionList.get(element).toByteArray();
			boolean pass = true;
			
			for(int i=0; i<m_attributeSize; i++)
			{
				//System.out.println("test " + i);
				if(conditionList[i]!=1)
					continue;
				
				if(originalElement[i]!=testCase[i])
				{
					pass = false;
					break;
				}
			}
			
			if(pass)
			{
				result.add(m_elementDecision.get(element));
			}
		}
		
		return result.toArray();
	}

}
