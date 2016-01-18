package org.sparkpipe.spark.netflow

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{HadoopFsRelation, HadoopFsRelationProvider}
import org.apache.spark.sql.types.StructType

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
