package com.mapreduce.advance.invertindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class InvertIndex implements WritableComparable<InvertIndex> {
	
	private String word;
	private String file;
	private Integer lineNum;
	private Integer times;

	public InvertIndex() {
		super();
	}

	public InvertIndex(String word, String file, Integer lineNum, Integer times) {
		super();
		this.word = word;
		this.file = file;
		this.lineNum = lineNum;
		this.times = times;
	}

	@Override
	public String toString() {
		return "InvertIndex [word=" + word + ", file=" + file + ", lineNum=" + lineNum + ", times=" + times + "]";
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public Integer getLineNum() {
		return lineNum;
	}

	public void setLineNum(Integer lineNum) {
		this.lineNum = lineNum;
	}

	public Integer getTimes() {
		return times;
	}

	public void setTimes(Integer times) {
		this.times = times;
	}

	public void readFields(DataInput in) throws IOException {
		
		word = in.readUTF();
		file = in.readUTF();
		lineNum = in.readInt();
		times = in.readInt();
		
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(word);
		out.writeUTF(file);
		out.writeInt(lineNum);
		out.writeInt(times);
		
	}

	public int compareTo(InvertIndex o) {
		int result = o.getFile().compareTo(this.getFile());
		if (result == 0) {
			result = o.getWord().compareTo(this.getWord());
			if (result == 0) {
				result = o.getLineNum() - this.getLineNum();
			}
		}
		
		return result;
	}

}
