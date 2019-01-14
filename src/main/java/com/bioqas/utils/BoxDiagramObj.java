package com.bioqas.utils;



/**
 * ����ͼ����
 * @author user
 *
 */
public class BoxDiagramObj {
	
	    //���ķ�λ��
	     private  double _Q3;
	     //��λ��
	     private double _Q2;
	     //�ķ�λ���
	     private double _IQR;
	     //���ķ�λ��
	     private double _Q1;
	     //�ϱ�Ե
	     private double  _UpperEdge;
	     //�±�Ե
	     private double  _LowerEdge;
	     //��ֵ
	     private double  _mean;
	     
	     public   BoxDiagramObj(){
	    	   _Q1 = 0;
	           _Q2 = 0;
	           _Q3 = 0;
	           _IQR= 0;
	     }
	     
	     public void setQ3(double Q3){
	           this._Q3=Q3;
	     }
	     public void setQ2( double Q2){
	           this._Q2=Q2;
	     }
	     public void setIQR( double IQR){
	           this._IQR=IQR;
	     }
	     
	     public void setQ1( double Q1){
	           this._Q1=Q1;
	     }
	     
	     public double getQ3(){
	           return _Q3;
	     }
	     public double getQ2(){
	          return _Q2;
	     }
	     
	  
	     public double getQ1(){
	           return _Q1;
	     }
	     
	     public double getIQR(){
	           return  _IQR;
	     }
	    
	     public double getUpperEdge(){
	           return _UpperEdge;
	     }
	     
	     public double getLowerEdge(){
	    	 return _LowerEdge;
	     }
	     public void setUpperEdge( double UpperEdge){
	           this._UpperEdge=UpperEdge;
	     } public void setLowerEdge( double LowerEdge){
	           this._LowerEdge=LowerEdge;
	     }
	     public double getMean(){
	    	 return _mean;
	     }
	     public void setMean( double Mean){
	           this._mean=Mean;
	     } 

	     
}
