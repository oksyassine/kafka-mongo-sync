/**
 * 
 */
package Main;


import Local.ProducerLocal;

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
		
		Local.ConsumerLocal consumerLocal=new Local.ConsumerLocal();
		
		ProducerLocal producerLocal=new ProducerLocal();
		
		producerLocal.producer_local.append("key", "producer_local");
		
		consumerLocal.consumer_local.append("key", "consumer_local");
		
		consumerLocal.start();
		
		producerLocal.start();
		
    	
		

	}

}
