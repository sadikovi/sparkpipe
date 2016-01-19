package org.sparkpipe.spark.netflow

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.Logging
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources.{HadoopFsRelation, HadoopFsRelationProvider, OutputWriterFactory}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

class DefaultSource extends HadoopFsRelationProvider {
    /**
     * Create relation for Netflow data. Options include path to the Netflow files, flow version
     * of the files, e.g. V5, V7, etc. All files must be of the same version.
     */
    def createRelation(
        sqlContext: SQLContext,
        paths: Array[String],
        dataSchema: Option[StructType],
        partitionColumns: Option[StructType],
        parameters: Map[String, String]
    ): HadoopFsRelation = {
        new NetflowRelation(paths, dataSchema, partitionColumns, parameters)(sqlContext)
    }
}

private[netflow] class NetflowRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String]
)(@transient val sqlContext: SQLContext) extends HadoopFsRelation with Logging {
    // extract Netflow version (currently only version 5 is supported)
    private val version = parameters.getOrElse("version",
        sys.error("'version' must be specified for Netflow data"))
    SchemaResolver.validateVersion(version)

    override def dataSchema: StructType = SchemaResolver.getSchemaForVersion(version)

    override def buildScan(
        requiredColumns: Array[String],
        inputFiles: Array[FileStatus]
    ): RDD[Row] = {
        if (inputFiles.isEmpty) {
            sqlContext.sparkContext.emptyRDD[Row]
        } else {
            // internal Netflow fields
            val mapper = SchemaResolver.getMapperForVersion(version)
            val fields: Array[Long] = if (requiredColumns.isEmpty) {
                mapper.getInternalColumns()
            } else {
                requiredColumns.map(col => mapper.getInternalColumnForName(col))
            }

            new UnionRDD[Row](sqlContext.sparkContext, inputFiles.map { status =>
                new NetflowRDD(sqlContext.sparkContext, status.getPath.toString, 1)
            })
        }
    }

    override def prepareJobForWrite(job: Job): OutputWriterFactory = {
        throw new UnsupportedOperationException("Write is not supported for Netflow")
    }
}
