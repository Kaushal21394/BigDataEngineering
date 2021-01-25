package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text,Text,Text,Text>{
	
	public static final Text EMPTY_TEXT = new Text("");
	private Text temp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		joinType = context.getConfiguration().get("join.type");
	}
	
	@Override
	public void reduce(Text key,Iterable<Text> value,Context context)throws IOException, InterruptedException {
		
		listA.clear();
		listB.clear();
		
		for(Text val:value) {
			temp = val;
			
			if(temp.charAt(0)=='A') {
				listA.add(new Text(temp.toString().substring(1)));
			}
			else if(temp.charAt(0)=='B') {
				listB.add(new Text(temp.toString().substring(1)));	
			}
			
		}
		executeJoin(context);
		
	}
	
	private void executeJoin(Context context)throws IOException, InterruptedException {
		if(joinType.equalsIgnoreCase("inner")) {
			if(!listA.isEmpty() && !listB.isEmpty()){
                for(Text A: listA){
                    for(Text B: listB){
                        //System.out.println("ListAB contains : "+ A + " " + B);
                        context.write(A, B);
                    }
                }
			}
		}
	}
}
