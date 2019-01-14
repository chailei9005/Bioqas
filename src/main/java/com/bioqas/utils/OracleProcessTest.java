package com.bioqas.utils;

import com.bioqas.test.OracleUtilsOptimize;

/**
 * Created by chailei on 18/9/7.
 */
public class OracleProcessTest {

    public static void main(String[] args) {

        for(int i = 0 ;i<70;i++){
            new Thread(new Runnable() {
                public void run() {
                    System.out.println(Thread.currentThread().getName()+" running");
                    OracleUtilsOptimize.getConnect();
                    try {
                        Thread.sleep(1000000000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }


    }
}
