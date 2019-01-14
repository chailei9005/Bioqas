package com.bioqas.utils;

public class BoxDiagram {
	 
	/**
	 * ����ͼ���
	 * @param �������
	 * @return ����ͼ��ݶ���
	 * @author Zyf
	 */
    public static BoxDiagramObj RobustStatisticsQX(double[] pArray)
    {
        double _Q1 = 0;
        double _Q2 = 0;
        double _Q3 = 0;
        double _IQR = 0;
        double _mean=0;
        BoxDiagramObj   _BoxDiagramObj= new BoxDiagramObj();
        try
        {
            int pCount = pArray.length;
            pArray = YDbasicMaths.BubbleSort(pArray);

            if ((pCount + 1) % 4 == 0)//��n+1��Ϊ4�ı���
            {
                _Q1 = pArray[(pCount + 1) / 4 - 1];
                _Q2 = pArray[2 * (pCount + 1) / 4 - 1];
                _Q3 = pArray[3 * (pCount + 1) / 4 - 1];
            }
            else //��n+1������4�ı���
            {
                double _intIndexQ1 = (pCount + 1) / 4.0;
                double _intIndexQ2 = 2 * (pCount + 1) / 4.0;
                double _intIndexQ3 = 3 * (pCount + 1) / 4.0;
                int _Q1f = (int)(Math.floor(_intIndexQ1));
                int _Q2f = (int)(Math.floor(_intIndexQ2));
                int _Q3f = (int)(Math.floor(_intIndexQ3));
                _Q1 = pArray[_Q1f - 1] * (1 - (_intIndexQ1 - _Q1f)) + pArray[_Q1f] * (_intIndexQ1 - _Q1f);
                _Q2 = pArray[_Q2f - 1] * (1 - (_intIndexQ2 - _Q2f)) + pArray[_Q2f] * (_intIndexQ2 - _Q2f);
                _Q3 = pArray[_Q3f - 1] * (1 - (_intIndexQ3 - _Q3f)) + pArray[_Q3f] * (_intIndexQ3 - _Q3f);

            }
           
            _IQR = _Q3 - _Q1;
            _mean=YDbasicMaths.Mean(pArray);
            _BoxDiagramObj.setQ1(_Q1);
            _BoxDiagramObj.setQ2(_Q2);
            _BoxDiagramObj.setQ3(_Q3);
            _BoxDiagramObj.setIQR(_IQR); 
            _BoxDiagramObj.setUpperEdge(_Q3+1.5*_IQR);  
            _BoxDiagramObj.setLowerEdge(_Q1-1.5*_IQR); 
            _BoxDiagramObj.setMean(_mean);
            
        }
        catch (Exception e)
        {
            try {
                throw e;
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        return _BoxDiagramObj;
    }
}
