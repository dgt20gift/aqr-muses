package com.renault.rntbci.aqr.appln.jms.service.impl;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.slf4j.Logger;

import com.renault.rntbci.aqr.appln.jms.service.JmsMessageSenderService;
import com.renault.rntbci.aqr.common.utils.DateUtil;
import com.renault.rntbci.aqr.login.data.UserObj;
import com.renault.rntbci.aqr.rating.data.RatingDefectObj;

/**
 * The Class JmsMessageSenderServiceImpl.
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
public class JmsMessageSenderServiceImpl implements JmsMessageSenderService {

	/** The log. */
	@Inject private Logger log;
	
	/** The queue. */
	@Resource(name="queue", mappedName="java:jboss/client/jms/UDAQRGREMUSES")
	Queue queue;
	
	/** The queue factory. */
	@Resource(name="factory", mappedName="java:jboss/client/jms/Factory")
	QueueConnectionFactory queueFactory;	
	
	/**
	 * Send message.
	 *
	 * @param ratingDefectVO the rating defect vo
	 * @param userObj the user obj
	 * @param check the check
	 */
	@Override
	public void sendMessage(RatingDefectObj ratingDefectVO, UserObj userObj,String check) {
		QueueConnection queueConnection = null;
        QueueSession queueSession = null;
        QueueSender queueSender = null;
        try {       
	         queueConnection = queueFactory.createQueueConnection();
	         queueSession = queueConnection.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
	         queueSender = queueSession.createSender(queue);
	         TextMessage   message = queueSession.createTextMessage();
	         String text = constructMessage(ratingDefectVO,userObj,check);
	         message.setText(text);
	         queueSender.send(message);
	         log.info("Send message finished...Message Content :: >>"+text);
        } catch (JMSException e) {        	
        	logTrace(e); 
        	logError(e);
		}finally {
		    	  closeSender(queueSender);
		    	  closeSession(queueSession);
		          closeConnection(queueConnection);
		}	
	}
	
	private void logTrace(Exception exe) {
		StringBuilder result = new StringBuilder();        
	    String lineSeperator = System.getProperty("line.separator");
		for(String stackTrace : ExceptionUtils.getRootCauseStackTrace(exe)) {
			if(StringUtils.contains(stackTrace, "Exception")||(StringUtils.contains(stackTrace, "com.renault.rntbci.aqr") && !(StringUtils.contains(stackTrace, "Weld")))){
				result.append(stackTrace);
	    		result.append(lineSeperator);
	    		   		
	    	} 			
	    }	
		log.info("Exception in sending MQ Message to GQU:"+result.toString()); 
	}
	
	private void logError(Exception exe) {
		Throwable throwable = ExceptionUtils.getRootCause(exe);
	    if (throwable instanceof JMSException) {
	    	log.error(((JMSException) throwable).getLocalizedMessage());
	    }else{
	    	log.error(exe.getMessage());
	    }
	}

	/**
	 * Close sender.
	 *
	 * @param queueSender the queue sender
	 */
	public void closeSender(QueueSender queueSender){    
    	if (queueSender != null) {
			try {
				queueSender.close();
			} catch (JMSException jme) {
				logTrace(jme); 
	        	logError(jme);
			}
			
		}
	}
	
	/**
	 * Close session.
	 *
	 * @param queueSession the queue session
	 */
	public void closeSession(QueueSession queueSession){    
    	if (queueSession != null) {
			try {
				queueSession.close();
			} catch (JMSException jme) {
				logTrace(jme); 
	        	logError(jme);
			}
			
		}
	}
	
	/**
	 * Close connection.
	 *
	 * @param queueConnection the queue connection
	 */
	public void closeConnection(QueueConnection queueConnection){    
    	if (queueConnection != null) {
			try {
				queueConnection.close();
			} catch (JMSException jme) {
				logTrace(jme); 
	        	logError(jme);
			}
			
		}
	}
	
	/**
	 * Construct message.
	 *
	 * @param ratingDefectVO the rating defect vo
	 * @param userObj the user obj
	 * @param check the check
	 * @return the string
	 */
	private static String constructMessage(RatingDefectObj ratingDefectVO,UserObj userObj,String check){
		StringBuilder sb = new StringBuilder();
		String message;
			if((ratingDefectVO.getIdentifierNumber().length()) <= 12){
					message = String.format("%s%-6.6s%-12.12s%17.17s%-4s%-4s%-6.6s%-80.80s%-3s%-3s%-3s%-1s%-3s%-2s%-2s%-80.80s%-7s%-4.4s%-1s%-1s%s%s", check, userObj.getStandards(),ratingDefectVO.getIdentifierNumber()," ",ratingDefectVO.getElement(), ratingDefectVO.getIncident(), ratingDefectVO.getProdStepCode(), ratingDefectVO.getDemerit(), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel1()), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel2()), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel3()) ,StringUtils.trimToEmpty(ratingDefectVO.getChargingTeam()), ratingDefectVO.getPlantCode(), StringUtils.trimToEmpty(ratingDefectVO.getLocalizationX()),  StringUtils.trimToEmpty(ratingDefectVO.getLocalizationY()), StringUtils.trimToEmpty(ratingDefectVO.getComment()), StringUtils.upperCase(userObj.getIpn()), StringUtils.trimToEmpty(ratingDefectVO.getWeldSpotNumber()), ratingDefectVO.getTestType(), ratingDefectVO.getDefectType(),DateUtil.getInstance().dateToStringFormatMessageDate(DateUtil.getInstance().getCurrentDate()),DateUtil.getInstance().dateToStringFormatMessageDate(DateUtil.getInstance().getCurrentDate()));
			}else{
					message = String.format("%s%-6.6s%12.12s%-17.17s%-4s%-4s%-6.6s%-80.80s%-3s%-3s%-3s%-1s%-3s%-2s%-2s%-80.80s%-7s%-4.4s%-1s%-1s%s%s", check, userObj.getStandards(), " ", ratingDefectVO.getIdentifierNumber(), ratingDefectVO.getElement(), ratingDefectVO.getIncident(), ratingDefectVO.getProdStepCode(), ratingDefectVO.getDemerit(), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel1()), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel2()), StringUtils.trimToEmpty(ratingDefectVO.getAllocationLevel3()),StringUtils.trimToEmpty(ratingDefectVO.getChargingTeam()), ratingDefectVO.getPlantCode(), StringUtils.trimToEmpty(ratingDefectVO.getLocalizationX()),  StringUtils.trimToEmpty(ratingDefectVO.getLocalizationY()), StringUtils.trimToEmpty(ratingDefectVO.getComment()), StringUtils.upperCase(userObj.getIpn()), StringUtils.trimToEmpty(ratingDefectVO.getWeldSpotNumber()), ratingDefectVO.getTestType(), ratingDefectVO.getDefectType(),DateUtil.getInstance().dateToStringFormatMessageDate(DateUtil.getInstance().getCurrentDate()),DateUtil.getInstance().dateToStringFormatMessageDate(DateUtil.getInstance().getCurrentDate()));
			}
		sb.append(message);
		return sb.toString();
	} 
}
