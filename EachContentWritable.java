package chaohParse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class EachContentWritable implements WritableComparable<EachContentWritable>{
	//private String Content ;
    private String title ;
    private String pos;
    private int count;
    
    public EachContentWritable(){}
    /*------ set function ------*/
    public void setPos(String x) { this. pos = x;}
    public void setTitle(String x) { this. title = x;}
    public void setCount(int x) { this. count = x;}
    /*------ get function ------*/
    public String getPos() { return pos ;}
    public String getTitle() { return title ;}
    public int getCount() { return count ;}
    
    @Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		pos = in.readUTF();
		title = in.readUTF();
		count = in.readInt();
	}
    @Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(pos);
		out.writeUTF(title);
		out.writeInt(count);
	}
    @Override
	public int compareTo(EachContentWritable arg0) {
		// TODO Auto-generated method stub
		int tmp = title .compareTo(arg0. title);
        if ( tmp != 0) return tmp;
        return pos .compareTo(arg0. pos);
	}
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return title + ":" + pos ;
    }
}