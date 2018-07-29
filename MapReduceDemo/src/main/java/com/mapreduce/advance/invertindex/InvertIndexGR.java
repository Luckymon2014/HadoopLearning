package com.mapreduce.advance.invertindex;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertIndexGR extends WritableComparator {

	// 作用在reduce做group by的时候
	// 保证在排序之后同一个key都进入了同一个reduce
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		InvertIndex indexA = (InvertIndex) a;
		InvertIndex indexB = (InvertIndex) b;
		
		int result = indexA.getFile().compareTo(indexB.getFile());
		if (result == 0) {
			result = indexA.getWord().compareTo(indexB.getWord());
			if (result == 0) {
				result = indexA.getLineNum() - indexB.getLineNum();
			}
		}
		
		return result;
		
	}
	
}
