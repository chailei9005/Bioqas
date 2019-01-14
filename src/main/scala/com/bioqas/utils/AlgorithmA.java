package com.bioqas.utils;


/**
 * 稳健均值和稳健标准差
 */
public class AlgorithmA
{

	/**
	 *  �㷨A
	 * @author Zyf
	 *
	 */
  public  static RetRobustObj RobustStatisticsA(double[] pArray) throws Exception {
        double _RobustMean = 0;
        double _RobustSD = 0;
        double _Mean = 0;
        double _Mean2 = 0;
        double _R = 0;
        RetRobustObj   _RetRobustObj= new RetRobustObj();
        try
        {
            int pCount = pArray.length;
            pArray = YDbasicMaths.BubbleSort(pArray);
            _Mean = YDbasicMaths.GetMedian(pArray);
            double[] _MpArray = new double[pCount];
            double[] _MpArrayX = new double[pCount];
            //foreach (double testData in _CalpArray)
            for (int i = 0; i < pCount; i++)
            {
                _MpArray[i] = Math.abs(pArray[i] - _Mean);

            }
            _Mean2 = YDbasicMaths.GetMedian(_MpArray);
            _R = 1.483 * _Mean2 * 1.5;
            for (int i = 0; i < pCount; i++)
            {
                if ((_Mean - _R) > pArray[i])
                {
                    _MpArrayX[i] = _Mean - _R;
                }
                else if ((_Mean + _R) < pArray[i])
                {
                    _MpArrayX[i] = _Mean + _R;
                }
                else
                {
                    _MpArrayX[i] = pArray[i];
                }
            }
            double _SumX = 0;
            for (int i = 0; i < pCount; i++)
            {
                _SumX = _MpArrayX[i] + _SumX;
            }
            _RobustMean = _SumX / pCount;
            double _SumE = 0;
            for (int i = 0; i < pCount; i++)
            {
                _SumE = (_MpArrayX[i] - _RobustMean) * (_MpArrayX[i] - _RobustMean) + _SumE;
            }
            _RobustSD = Math.sqrt(_SumE / (pCount - 1)) * 1.134;

            _RetRobustObj.setRobustMean( _RobustMean);
            _RetRobustObj.setRobustSD( _RobustSD);
         
        }
        catch (Exception e)
        {
            throw e;
        }
        return _RetRobustObj;
    }

  
}

