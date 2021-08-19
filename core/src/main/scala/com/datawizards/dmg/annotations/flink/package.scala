package com.datawizards.dmg.annotations

import scala.annotation.StaticAnnotation

/**
  * HIVE DDL dedicated annotations
  */
package object flink {

  /**
    * Generated Hive DLL should be EXTERNAL table
    *
    */
  final class flinkTableProperty extends StaticAnnotation

  /**
    * Flink TBLPROPERTIES section
    *
    * @param key key of table property
    * @param value value of table property
    */
  final class flinkPartitionColumn(val order: Int=0) extends StaticAnnotation
}
