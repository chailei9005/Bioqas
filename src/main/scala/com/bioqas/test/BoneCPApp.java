package com.bioqas.test;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by chailei on 18/9/18.
 */
public class BoneCPApp {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("oracle.jdbc.driver.OracleDriver");     // load the DB driver
        BoneCPConfig config = new BoneCPConfig();    // create a new configuration object
        // set the JDBC url
        config.setJdbcUrl("jdbc:oracle:thin:@47.92.136.8:1521:biodev");
        config.setUsername("biodev");            // set the username
        config.setPassword("Bio#2018#Ora");                // set the password

        //一些参数设置
        config.setPartitionCount(3);
        config.setMaxConnectionsPerPartition(20);
        config.setMinConnectionsPerPartition(10);
        config.setAcquireIncrement(5);
        config.setPoolAvailabilityThreshold(20);
        config.setReleaseHelperThreads(2);
        config.setIdleMaxAge(240, TimeUnit.MINUTES);
        config.setIdleConnectionTestPeriod(60,TimeUnit.MINUTES);
        config.setStatementsCacheSize(20);
        config.setStatementReleaseHelperThreads(3);

        BoneCP connectionPool = new BoneCP(config);     // setup the connection pool

        Connection connection = connectionPool.getConnection();     // fetch a connection

        String sql = "select * from TP_E_BD_REPORT_DATE";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
        ps.close();

        connection.close();                // close the connection
        connectionPool.shutdown();            // close the connection pool


    }
}
