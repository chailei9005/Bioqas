package com.bioqas.utils;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/// <summary>
/// ���㷨
/// </summary>
public class YDbasicMaths
{
 public YDbasicMaths() { }
    public static double Rounds(double dValue,int len){
    	 BigDecimal b = new BigDecimal(dValue);//BigDecimal ��ʹ�û�����ȫ����������Ϊ
         double retD = b.setScale(len, BigDecimal.ROUND_HALF_UP).doubleValue();
         return retD;
    }

    /// <summary>
    /// ���ֵ
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double Max(double[] oArray)
    {
        double retMax = 0;
        if (oArray.length < 1)
        { }
        else
        {
            retMax = BubbleSort(oArray)[oArray.length-1];
        }
        return retMax;
    }
    
    /// <summary>
    /// ���ֵ
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double Max(List<Double> oArray)
    {
        double retMax = 0;
        if (oArray.size() < 1)
        { }
        else
        {          
            retMax =BubbleSort(oArray).get(oArray.size() - 1);
        }
        return retMax;
    }

    /// <summary>
    /// ��ȡ��Сֵ
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double Min(List<Double> oArray)
    {
        double retMax = 0;
        if (oArray.size() < 1)
        {
        }
        else
        {
            retMax = BubbleSort(oArray).get(0);
        }
        return retMax;
    }
    
    
    /// <summary>
    /// ��ȡ��Сֵ
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double Min(double[] oArray)
    {
        double retMax = 0;
        if (oArray.length < 1)
        {
        }
        else
        {
            retMax = BubbleSort(oArray)[0];
        }
        return retMax;
    }

    /// <summary>
    /// ��ȡ��ֵ
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double Mean(double[] oArray)
    {
        double retMean = 0;
        if (oArray.length < 1)
        {
        }
        else
        {
            double sum = 0;
            for(int i=0; i<oArray.length;i++)  
            {
                sum = sum + oArray[i];
            }
           
            retMean = Rounds(sum / oArray.length, 6);
        }
        return retMean;
    }

	/// <summary>
	/// ��ȡ��ֵ
	/// </summary>
	/// <param name="oArray">����</param>
	/// <returns></returns>
	public static double Mean(List<Double> oArray) {
		double retMean = 0;
		if (oArray==null||oArray.size() < 1) {
		} else {
			double sum = 0;
			for (Double _array : oArray) {
				sum = sum + _array;
			}
			retMean = Rounds(sum / oArray.size(), 6);
		}
		return retMean;
	}

    /// <summary>
    /// ��ȡ������λ��
    /// </summary>
    /// <param name="_Array">����</param>
    /// <returns>��λ��</returns>
    public static double GetMedian(double[] _Array)
    {

        double _Mean = 0;
        int _index = 0;
        double[] _CalpArray = BubbleSort(_Array);

        //���� ......��������������������
        int pCount = _CalpArray.length;
        if (pCount % 2 != 0)//����
        {
            _index = (pCount + 1) / 2;
            _Mean = _CalpArray[_index - 1];//��λ��
        }
        else  //ż��
        {
            _index = (pCount + 1) / 2 + (pCount) / 2;
            _Mean = (_CalpArray[(pCount) / 2 - 1]+_CalpArray[(pCount) / 2 ])/2;//��λ��
        }
       

        return _Mean;
    }

    /// <summary>
    /// ���׼��
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double SD(double[] oArray)
    {
        double retSD = 0;
        if (oArray.length <= 1)
        {
        }
        else
        {
            double avg = Mean(oArray);
            double sumstdev = 0;
            for(int i=0; i<oArray.length;i++)  
            {
                sumstdev = sumstdev + (oArray[i] - avg) * (oArray[i] - avg);
            }
            double stdeval =Math.sqrt(sumstdev / (oArray.length - 1));
            retSD =Rounds(stdeval, 6);
        }
        return retSD;
    }

    
    /// <summary>
    /// ���׼��
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double SD(List<Double> oArray)
    {
        double retSD = 0;
        
        if (oArray==null||oArray.size() <= 1)
        {
        }
        else
        {
            double avg = Mean(oArray);
            double sumstdev = 0;
            for(Double _array :oArray)
            {
            	 sumstdev = sumstdev + (_array - avg) * (_array - avg);
            }
            double stdeval =Math.sqrt(sumstdev / (oArray.size() - 1));
            retSD =Rounds(stdeval, 6);
        }
        return retSD;
    }
    /// <summary>
    /// ����ϵ��
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double CV(double[] oArray)
    {
        double retcv = 0;
        if (oArray.length < 1)
        {
        }
        else
        {
            retcv = SD(oArray) / Mean(oArray) * 100;
        }
        return retcv;
    }



    /// <summary>
    /// ð������
    /// </summary>
    /// <param name="oArray">����</param>
    /// <returns></returns>
    public static double[] BubbleSort(double[] oArray)
    {
        double[] _sortArray;
        int i, j; double temp; //������־ 
        boolean exchange;
        for (i = 0; i < oArray.length; i++) //�����R.length-1������ 
        {
            exchange = false; //��������ʼǰ��������־ӦΪ��
            for (j = oArray.length - 2; j >= i; j--)
            {
            	///��������
            	if(oArray[j + 1] < oArray[j])
            	{
            		temp = oArray[j + 1];
                  oArray[j + 1] = oArray[j];
                  oArray[j] = temp;
                  exchange = true; //�����˽������ʽ�������־��Ϊ�� 
            	}
            }
            if (!exchange) //��������δ��������ǰ��ֹ�㷨 
            {
                break;
            }
        }
        _sortArray = oArray;
        return _sortArray;
    }

    /// <summary>
    /// ð������
    /// </summary>
    /// <param name="oArray">List<Double> ����</param>
    /// <returns></returns>
    public static List<Double> BubbleSort(List<Double> oArray)
    {
    	List<Double> _sortArray;
        int i, j; double temp; //������־ 
        boolean exchange;
        for (i = 0; i < oArray.size(); i++) //�����R.length-1������ 
        {
            exchange = false; //��������ʼǰ��������־ӦΪ��
            for (j = oArray.size() - 2; j >= i; j--)
            {
            	///��������
            	if(oArray.get(j + 1) < oArray.get(j))
            	{
            		temp = oArray.get(j + 1);
                  oArray.set( (j + 1),oArray.get(j));
                  oArray.set(j, temp);
                  exchange = true; //�����˽������ʽ�������־��Ϊ�� 
            	}
            }
            if (!exchange) //��������δ��������ǰ��ֹ�㷨 
            {
                break;
            }
        }
        _sortArray = oArray;
        return _sortArray;
    }
    /// <summary>
    /// ѡ������
    /// </summary>
    /// <param name="oArray"></param>
    public static double[] SelectionSorter(double[] oArray)
    {
        double[] _sortArray = null;
        if (oArray.length < 1)
        {
        }
        else
        {
            int min;
            for (int i = 0; i < oArray.length - 1; ++i)
            {
                min = i;
                for (int j = i + 1; j < oArray.length; ++j)
                {
                    if (oArray[j] < oArray[min])
                        min = j;
                }
                double t = oArray[min];
                oArray[min] = oArray[i];
                oArray[i] = t;
            }
            _sortArray = oArray;
        }
        return _sortArray;
    }

    /// <summary>
    /// ϣ������
    /// </summary>
    /// <param name="list"></param>
    public static double[] ShellSorter(double[] oArray)
    {
        double[] _sortArray = null;
        if (oArray.length < 1)
        {
        }
        else
        {
            int inc;
            for (inc = 1; inc <= oArray.length / 9; inc = 3 * inc + 1) ;
            for (; inc > 0; inc /= 3)
            {
                for (int i = inc + 1; i <= oArray.length; i += inc)
                {
                    double t = oArray[i - 1];
                    int j = i;
                    while ((j > inc) && (oArray[j - inc - 1] > t))
                    {
                        oArray[j - 1] = oArray[j - inc - 1];
                        j -= inc;
                    }
                    oArray[j - 1] = t;
                }
            }
            _sortArray = oArray;
        }
        return _sortArray;
    }

    /// <summary>
    /// ��������
    /// </summary>
    /// <param name="list"></param>
    public static double[] InsertionSorter(double[] oArray)
    {

        double[] _sortArray = null;
        if (oArray.length < 1)
        {
        }
        else
        {
            for (int i = 1; i < oArray.length; ++i)
            {
                double t = oArray[i];
                int j = i;
                while ((j > 0) && (oArray[j - 1] > t))
                {
                    oArray[j] = oArray[j - 1];
                    --j;
                }
                oArray[j] = t;
            } _sortArray = oArray;
        }
        return _sortArray;
    }
    
    /***
	 * ��������ʵ����SDI
	 * @param myMean ���Ҿ�ֵ
	 * @para allLabMean ����ʵ���Ҿ�ֵ
	 * @param allLabSD �ܱ�׼��
	 * @return
	 */
	@SuppressWarnings("finally")
	public static Double CarALLSDI(double myMean, double allLabMean, double allLabSD) {
		Double methodSDI = null;
		if (Double.isNaN(allLabSD)||Double.isNaN(allLabMean)||Double.isNaN(myMean)|| allLabSD == 0) {
			return methodSDI;
		}
		try {
			methodSDI = (myMean - allLabMean) / allLabSD;
		} catch (Exception e) {
			throw e;
		} finally {
			return methodSDI;
		}
	}
	
	
 /***
  * ��������ʵ����CVI
  * @param mySD ���ұ�׼��
  * @param allLabSD �ܱ�׼���
  * @return
  */
	@SuppressWarnings("finally")
	public static Double CarALLCVI( double mySD, double allLabSD) {
		double methodCVI = 0;
		if (Double.isNaN(allLabSD)||Double.isNaN(methodCVI)|| allLabSD == 0) {
			return methodCVI;
		}
		try {
		methodCVI = mySD / allLabSD;
		} catch (Exception e) {
			throw e;
		} finally {
			return methodCVI;
		}
	}


	
	
	/***
	 * ���㷽����SDI
	 * @param myMean ���Ҿ�ֵ
	 * @para methodGroupMean ����ʵ���Ҿ�ֵ
	 * @param methodGroupSD ����
	 * @return
	 */
	@SuppressWarnings("finally")
	public static Double CarMethodSDI(double myMean, double methodGroupMean, double methodGroupSD) {
		Double methodSDI = null;
		if (Double.isNaN(myMean)||Double.isNaN(methodGroupMean)|| Double.isNaN(methodGroupSD)|| methodGroupSD == 0) {
			return methodSDI;
		}
		try {
			methodSDI = (myMean - methodGroupMean) / methodGroupSD;
		} catch (Exception e) {
			throw e;
		} finally {
			return methodSDI;
		}
	}
	
	
 /***
  * ���㷽����CVI
  * @param mySD ���ұ�׼��
  * @param methodGroupSD �������׼��
  * @return
  */
	@SuppressWarnings("finally")
	public static Double CarMethodCVI( double mySD, double methodGroupSD) {
		double methodCVI = 0;
		if (Double.isNaN(mySD)||Double.isNaN(methodGroupSD)|| methodGroupSD == 0) {
			return methodCVI;
		}
		try {
		methodCVI = mySD / methodGroupSD;
		} catch (Exception e) {
			throw e;
		} finally {
			return methodCVI;
		}
	}    
    
}
