package com.bioqas.utils;

/**
 *  �Ƚ�ͳ���㷨������ 
 *  
 * @author Zyf
 *
 */
public class RetRobustObj {
	  public RetRobustObj(){}
		
	     private  double _RobustMean;
	     private double _RobustSD;
	     public void setRobustMean(double RobustMean){
	           this._RobustMean=RobustMean;
	     }
	     public void setRobustSD( double RobustSD){
	           this._RobustSD=RobustSD;
	     }
	     public double getRobustMean(){
	           return _RobustMean;
	     }
	     public double getRobustSD(){
	          return _RobustSD;
	     }
}
