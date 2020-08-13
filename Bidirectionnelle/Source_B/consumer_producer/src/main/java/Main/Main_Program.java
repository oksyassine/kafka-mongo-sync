/**
 * 
 */
package Main;


import Distant.ConsumerDistant;



/**
 * @author Ardouz
 *
 */
public class Main_Program {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Distant.ProducerDistant producerDistant=new Distant.ProducerDistant();
		ConsumerDistant consumerDistant=new ConsumerDistant();
		consumerDistant.consumer_distant.append("key", "consumer_distant");
		
		producerDistant.producer_distant.append("key", "producer_distant");
		
		producerDistant.start();
		
		consumerDistant.start();
		
		
    	
		

	}

}
