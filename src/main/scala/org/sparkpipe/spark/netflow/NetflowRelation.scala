package org.sparkpipe.spark.netflow

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriterFactory}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

private[netflow] class NetflowRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String]
)(@transient val sqlContext: SQLContext) extends HadoopFsRelation with Logging {
    // extract Netflow version (currently only version 5 is supported)
    private val version = parameters.getOrElse("version",
        sys.error("'version' must be specified for Netflow data"))
    // check whether we support this version or not
    SchemaResolver.validateVersion(version)

    override def dataSchema: StructType = SchemaResolver.getSchemaForVersion(version)

    override def buildScan(
        requiredColumns: Array[String],
        inputFiles: Array[FileStatus]
    ): RDD[Row] = {
        inputFiles.foreach(x => println(s"FileStatus: ${x}"))
        requiredColumns.foreach(x => println(s"RequiredColumn: ${x}"))

        if (inputFiles.isEmpty) {
            sqlContext.sparkContext.emptyRDD[Row]
        } else {
            sqlContext.sparkContext.emptyRDD[Row]
        }
    }

    override def prepareJobForWrite(job: Job): OutputWriterFactory = {
        throw new UnsupportedOperationException("Write is not supported for Netflow")
    }
}
