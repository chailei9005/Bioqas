package com.bioqas.utils;


import java.util.HashMap;
import java.util.Map;

/**
 * ��̫�ֲ�ͼ
 * 
 * @author user
 *
 */
public class NormalDistribution {

	public static String[][] NormalDistributionFun(double[] pArray) {
		String[][] _NormalDistributionObj = new String[7][2];
		try {
			double mean;
			double sd;
			double Positive3;
			double Positive2;
			double Positive1;
			double negative1;
			double negative2;
			double negative3;
			
			int intp3=0;
			int intp2=0;
			int intp1=0;
			int intm=0;
			int intn1=0;
			int intn2=0;
			int intn3=0;
			
			
			

			mean = YDbasicMaths.Mean(pArray);
			sd = YDbasicMaths.SD(pArray);
			// ����X�� ����
			Positive3 = mean + 3 * sd;
			Positive2 = mean + 2 * sd;
			Positive1 = mean + 1 * sd;
			negative1 = mean - 1 * sd;
			negative2 = mean - 2 * sd;
			negative3 = mean - 3 * sd;
		 
			_NormalDistributionObj[6][0] = "3sd("+String .format("%.2f",Positive3)+")";
			_NormalDistributionObj[5][0] = "2sd("+String .format("%.2f",Positive2)+")";
			_NormalDistributionObj[4][0] = "1sd("+String .format("%.2f",Positive1)+")";
			_NormalDistributionObj[3][0] = "avg("+String .format("%.2f",mean)+")";
			_NormalDistributionObj[2][0] = "-1sd("+String .format("%.2f",negative1)+")";
			_NormalDistributionObj[1][0] = "-2sd("+String .format("%.2f",negative2)+")";
			_NormalDistributionObj[0][0] = "-3sd("+String .format("%.2f",negative3)+")";

			for (int index = 0; index < pArray.length; index++) {
				if (pArray[index] > Positive3) {//>3
					intp3=intp3+1;
//					_NormalDistributionObj[6][1] = Integer.parseInt( _NormalDistributionObj[6][0]) + 1;
				} else if (pArray[index] < negative3) {//<-3
					intn3=intn3+1;
//					_NormalDistributionObj[0][1] = _NormalDistributionObj[0][1] + 1;
				} else if ((pArray[index] <= Positive3) && (pArray[index] > Positive2)) {//3>=X>2
					intp2=intp2+1;
//					_NormalDistributionObj[1][1] = _NormalDistributionObj[1][1] + 1;
				}
				else if ((pArray[index] <= Positive2) && (pArray[index] > Positive1)) {//2>=X>1
					intp1=intp1+1;
//					_NormalDistributionObj[2][1] = _NormalDistributionObj[2][1] + 1;
				}
				else if ((pArray[index] <= Positive1) && (pArray[index] >= negative1)) {//1>=X>=-1
					intm=intm+1;
//					_NormalDistributionObj[3][1] = _NormalDistributionObj[3][1] + 1;
				}
				else if ((pArray[index] < Positive1) && (pArray[index] >= negative2)) {//-1>X>=-2
					intn1=intn1+1;
//					_NormalDistributionObj[4][1] = _NormalDistributionObj[4][1] + 1;
				}
				else if ((pArray[index] < Positive2) && (pArray[index] >= negative3)) {//-2>X>=-3
					intn2=intn2+1;
//					_NormalDistributionObj[5][1] = _NormalDistributionObj[5][1] + 1;
				}
			}
			_NormalDistributionObj[6][1] = intp3+"";
							_NormalDistributionObj[5][1] =  intp2+"";
									_NormalDistributionObj[4][1] =  intp1+"";
											_NormalDistributionObj[3][1] = intm+"";
													_NormalDistributionObj[2][1] =  intn1+"";
															_NormalDistributionObj[1][1] =  intn2+"";
															_NormalDistributionObj[0][1] = intn3+"";
		 
			
		} catch (Exception e) {
			try {
				throw e;
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return _NormalDistributionObj;

	}
}
