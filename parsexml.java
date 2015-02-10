package chaohParse;

public class parsexml {
	
	public static String getContentlineFromXML(String row){
	String text=null;
	//String result_String = new String();
	int start,end;
	start=row.indexOf("<text>");
	if (start>=0) start=start+6;
	end = row.indexOf("</text>");
	
	text = row.substring (start,end);
	
	//result_String = text.replace("<p>", "");
	//result_String = result_String.replace("</p>", "");

	return text;
	}
}
