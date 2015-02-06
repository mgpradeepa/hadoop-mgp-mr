package in.co.impetus.usecases.Practice_Usecases.entity;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoCRSortComparator extends WritableComparator {
	protected TwoCRSortComparator() {
		super(NewCustomer.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		NewCustomer c1 = (NewCustomer) a;
		NewCustomer c2 = (NewCustomer) b;
		int result = 0;
		/**
		 * First start sorting on the customer id and and correspondingly sort
		 * the score in descending order. -1 * (x) => for descending order
		 */
		result = c1.getCustomerId().compareTo(c2.getCustomerId());
		if (result == 0) {
			result = c1.getLocation().compareToIgnoreCase(c2.getLocation());
			if(result ==0) {
//				if(c1.getRank() != 0 || c2.getRank() !=0){
//					c2.setRank(c2.getRank()+1);
//					c1.setRank(c1.getRank()-1);
//					
//				}else{
//					c1.setRank(1) ;
//				}
//				interScoreReult = c1.getScore().compareTo(c2.getScore());
//				// 0 cTo 9
//				if(interScoreReult == -1) {
//					if(c1.getRank() == 0 || c2.getRank() ==0){
//						c1.setRank(c1.getRank()+ c1.getRank()+ c1.getRank());
//						c2.setRank(c2.getRank()+ c2.getRank());
//					}
//					else {
//						c1.setRank(c1.getRank() + 1);
//						c2.setRank(c2.getRank() -1);
//					}
//				}
					
				
				return -1 * (c1.getScore().compareTo(c2.getScore()));
			}
		}
		return result;
	}
}
