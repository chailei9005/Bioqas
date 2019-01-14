package com.bioqas.test

import java.sql.Types

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
  * Created by chailei on 18/6/13.
  */
private case object OracleDialectSpeed extends JdbcDialect{
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:oracle")
  }

  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {

    println("size = " + size)
    println("sqlType =" + sqlType)
    if (sqlType == Types.NUMERIC) {
      val scale = if (null != md) md.build().getLong("scale") else 0L

      size match {
        // Handle NUMBER fields that have no precision/scale in special way
        // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
        // For more details, please see
        // https://github.com/apache/spark/pull/8780#issuecomment-145598968
        // and
        // https://github.com/apache/spark/pull/8780#issuecomment-144541760
        case 0 => Option(IntegerType)
        // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
        // this to NUMERIC with -127 scale
        // Not sure if there is a more robust way to identify the field as a float (or other
        // numeric types that do not specify a scale.
        case _ if scale == -127L => Option(IntegerType)
        case 1 => Option(StringType)
        case 3 | 5 | 10 => Option(IntegerType)
        case 19 if scale == 0L => Option(LongType)
        case 19 if scale == 4L => Option(FloatType)
        case _ => None
      }
    } else if(sqlType == Types.TIMESTAMP){
      Option(StringType)
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // For more details, please see
    // https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(200)", java.sql.Types.VARCHAR))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
