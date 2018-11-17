package cs522.part5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	private Double key;
	private String value;

	public Pair() {

	}

	public Pair(Double key, String value) {
		this.key = key;
		this.value = value;
	}

	public Double getKey() {
		return key;
	}

	public void setKey(Double key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public void set(Double key, String value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("(").append(key).append(", ")
				.append(value).append(")").toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readDouble();
		value = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(key);
		out.writeUTF(value);
	}

	@Override
	public int compareTo(Pair o) {
		return value.compareTo(o.value);
	}

}